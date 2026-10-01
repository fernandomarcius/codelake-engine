//! Catalog adapter that keeps listing metadata but resolves tables through a
//! connector-specific [`TableResolver`].
//!
//! Why it exists: generic catalog providers from `datafusion-table-providers`
//! hand out plain `SqlTable`s, which are *not* federated, so a join between two
//! tables of the same Postgres would pull both tables over the network. Routing
//! resolution through the connector's table factory returns federated providers,
//! and the whole sub-plan is pushed down to the source instead.
//!
//! A source whose objects change while the engine runs (a view created after
//! boot) is listed by a [`LiveListing`]: a name it lacks is looked up at the
//! source when a query names it, and the listing is read again when a listing
//! (`information_schema`, `SHOW`) asks and it is older than its time to live.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result;

/// Builds the provider for one table of a source.
#[async_trait]
pub trait TableResolver: Send + Sync + Debug {
    async fn resolve(&self, schema: &str, table: &str) -> Result<Option<Arc<dyn TableProvider>>>;
}

/// The listed name `name` refers to: itself when listed, else its lower-case
/// form. Listings hold the names the source folds to lower case (Trino's
/// rule), and a quoted identifier keeps its case (`"dbo_Vendas_CT"`, as dbt
/// writes every name), so it is resolved without regard to case; a name
/// listed with capitals (a `MongoDB` collection) still matches exactly.
fn listed_name(name: &str, listed: impl Fn(&str) -> bool) -> Option<String> {
    if listed(name) {
        return Some(name.to_owned());
    }
    let lower = name.to_lowercase();
    (lower != name && listed(&lower)).then_some(lower)
}

/// Catalog whose names come from `listing` and whose tables come from `resolver`.
#[derive(Debug)]
pub struct FederatedCatalog {
    listing: Arc<dyn CatalogProvider>,
    live: Option<LiveListing>,
    resolver: Arc<dyn TableResolver>,
}

impl FederatedCatalog {
    pub fn new(listing: Arc<dyn CatalogProvider>, resolver: Arc<dyn TableResolver>) -> Self {
        Self {
            listing,
            live: None,
            resolver,
        }
    }

    /// Catalog over a listing that follows the source (see [`LiveListing`]).
    pub fn live(listing: LiveListing, resolver: Arc<dyn TableResolver>) -> Self {
        Self {
            listing: Arc::new(listing.clone()),
            live: Some(listing),
            resolver,
        }
    }

    /// Reads the listing again when it follows the source and is older than
    /// its time to live; a fixed listing has nothing to read.
    ///
    /// # Errors
    ///
    /// The source's, when it cannot be listed.
    pub async fn refresh(&self) -> Result<()> {
        match &self.live {
            Some(live) => live.refresh().await,
            None => Ok(()),
        }
    }
}

impl CatalogProvider for FederatedCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.listing.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let Some(name) = listed_name(name, |n| self.listing.schema(n).is_some()) else {
            // A schema created after the listing was read: the next query
            // finds it once the listing is read again.
            if let Some(live) = &self.live {
                live.refresh_in_background();
            }
            return None;
        };
        self.listing.schema(&name).map(|listing| {
            Arc::new(FederatedSchema {
                name,
                listing,
                live: self.live.clone(),
                resolver: Arc::clone(&self.resolver),
            }) as Arc<dyn SchemaProvider>
        })
    }
}

/// Reads a source's names for a [`LiveListing`].
#[async_trait]
pub trait NameSource: Send + Sync + Debug {
    /// Every schema of the source with its tables.
    async fn list(&self) -> Result<BTreeMap<String, Vec<String>>>;

    /// The table of `schema` that `table` names at the source now, if any:
    /// `table` itself, else its lower-case form (the rule of the listing).
    async fn find(&self, schema: &str, table: &str) -> Result<Option<String>>;
}

/// Listing of a source whose objects change while the engine runs. A table
/// it lacks is looked up at the source when a query names it (and listed
/// from then on); the whole listing is read again on [`LiveListing::refresh`]
/// once older than its time to live, which a listing of the catalog
/// (`information_schema`, `SHOW`) and a lookup of an unlisted schema ask for.
#[derive(Debug, Clone)]
pub struct LiveListing {
    shared: Arc<Live>,
}

#[derive(Debug)]
struct Live {
    source: Arc<dyn NameSource>,
    ttl: Duration,
    names: RwLock<Names>,
    refreshing: AtomicBool,
}

#[derive(Debug)]
struct Names {
    schemas: BTreeMap<String, BTreeSet<String>>,
    read_at: Instant,
}

fn names_of(listing: BTreeMap<String, Vec<String>>) -> Names {
    Names {
        schemas: listing
            .into_iter()
            .map(|(schema, tables)| (schema, tables.into_iter().collect()))
            .collect(),
        read_at: Instant::now(),
    }
}

impl LiveListing {
    /// Reads the source's names once; `ttl` is how long they stand for the
    /// source before a listing reads them again.
    ///
    /// # Errors
    ///
    /// The source's, when it cannot be listed.
    pub async fn read(source: Arc<dyn NameSource>, ttl: Duration) -> Result<Self> {
        let names = names_of(source.list().await?);
        Ok(Self {
            shared: Arc::new(Live {
                source,
                ttl,
                names: RwLock::new(names),
                refreshing: AtomicBool::new(false),
            }),
        })
    }

    fn names<T>(&self, read: impl FnOnce(&Names) -> T) -> T {
        read(&self.shared.names.read().unwrap_or_else(PoisonError::into_inner))
    }

    fn stale(&self) -> bool {
        self.names(|names| names.read_at.elapsed() >= self.shared.ttl)
    }

    /// Reads the names again when they are older than the time to live.
    ///
    /// # Errors
    ///
    /// The source's, when it cannot be listed; the names read before stay.
    pub async fn refresh(&self) -> Result<()> {
        if !self.stale() {
            return Ok(());
        }
        let names = names_of(self.shared.source.list().await?);
        *self.shared.names.write().unwrap_or_else(PoisonError::into_inner) = names;
        Ok(())
    }

    /// [`LiveListing::refresh`] on its own task (one at a time), for callers
    /// that cannot wait; a failure is logged and the names read before stay.
    fn refresh_in_background(&self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if !self.stale() || self.shared.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        let listing = self.clone();
        runtime.spawn(async move {
            if let Err(error) = listing.refresh().await {
                tracing::warn!(%error, "listing of a source not read again");
            }
            listing.shared.refreshing.store(false, Ordering::Release);
        });
    }

    /// The table `table` names in `schema` at the source now, listed from
    /// then on.
    async fn find(&self, schema: &str, table: &str) -> Result<Option<String>> {
        let found = self.shared.source.find(schema, table).await?;
        if let Some(found) = &found {
            self.shared
                .names
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .schemas
                .entry(schema.to_owned())
                .or_default()
                .insert(found.clone());
        }
        Ok(found)
    }
}

impl CatalogProvider for LiveListing {
    fn schema_names(&self) -> Vec<String> {
        self.names(|names| names.schemas.keys().cloned().collect())
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.names(|names| names.schemas.contains_key(name)).then(|| {
            Arc::new(LiveSchema {
                listing: self.clone(),
                name: name.to_owned(),
            }) as Arc<dyn SchemaProvider>
        })
    }
}

/// A schema of a [`LiveListing`]: its tables as last read.
#[derive(Debug)]
struct LiveSchema {
    listing: LiveListing,
    name: String,
}

#[async_trait]
impl SchemaProvider for LiveSchema {
    fn table_names(&self) -> Vec<String> {
        self.listing.names(|names| {
            names
                .schemas
                .get(&self.name)
                .map(|tables| tables.iter().cloned().collect())
                .unwrap_or_default()
        })
    }

    async fn table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.listing.names(|names| {
            names
                .schemas
                .get(&self.name)
                .is_some_and(|tables| tables.contains(name))
        })
    }
}

/// Listing built from plain names, for sources without a native catalog provider
/// (`MongoDB` collections, for instance). It only answers "what exists"; tables are
/// always produced by the resolver of the wrapping [`FederatedCatalog`].
#[derive(Debug, Default)]
pub struct NameListing {
    schemas: BTreeMap<String, Arc<NameSchema>>,
}

impl NameListing {
    pub fn new(schemas: BTreeMap<String, Vec<String>>) -> Self {
        Self {
            schemas: schemas
                .into_iter()
                .map(|(name, tables)| (name, Arc::new(NameSchema { tables })))
                .collect(),
        }
    }
}

impl CatalogProvider for NameListing {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|schema| Arc::clone(schema) as Arc<dyn SchemaProvider>)
    }
}

#[derive(Debug)]
struct NameSchema {
    tables: Vec<String>,
}

#[async_trait]
impl SchemaProvider for NameSchema {
    fn table_names(&self) -> Vec<String> {
        self.tables.clone()
    }

    async fn table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.iter().any(|t| t == name)
    }
}

#[derive(Debug)]
struct FederatedSchema {
    name: String,
    listing: Arc<dyn SchemaProvider>,
    live: Option<LiveListing>,
    resolver: Arc<dyn TableResolver>,
}

#[async_trait]
impl SchemaProvider for FederatedSchema {
    fn table_names(&self) -> Vec<String> {
        self.listing.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(listed) = listed_name(name, |n| self.listing.table_exist(n)) {
            return self.resolver.resolve(&self.name, &listed).await;
        }
        // Created after the listing was read: the source knows.
        let Some(live) = &self.live else {
            return Ok(None);
        };
        match live.find(&self.name, name).await? {
            Some(found) => self.resolver.resolve(&self.name, &found).await,
            None => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        listed_name(name, |n| self.listing.table_exist(n)).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::datasource::empty::EmptyTable;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug, Default)]
    struct RecordingResolver {
        calls: Mutex<Vec<(String, String)>>,
    }

    fn table_with_column(name: &str) -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Int64,
            false,
        )]))))
    }

    #[async_trait]
    impl TableResolver for RecordingResolver {
        async fn resolve(&self, schema: &str, table: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            if let Ok(mut calls) = self.calls.lock() {
                calls.push((schema.to_owned(), table.to_owned()));
            }
            Ok(Some(table_with_column("resolved")))
        }
    }

    fn listing() -> Arc<dyn CatalogProvider> {
        let schema = MemorySchemaProvider::new();
        schema
            .register_table("orders".into(), table_with_column("listed"))
            .unwrap();
        let catalog = MemoryCatalogProvider::new();
        catalog.register_schema("public", Arc::new(schema)).unwrap();
        Arc::new(catalog)
    }

    #[tokio::test]
    async fn tables_come_from_the_resolver_not_the_listing() {
        let resolver = Arc::new(RecordingResolver::default());
        let catalog = FederatedCatalog::new(listing(), resolver.clone());

        assert_eq!(catalog.schema_names(), vec!["public"]);
        let schema = catalog.schema("public").unwrap();
        assert_eq!(schema.table_names(), vec!["orders"]);
        assert!(schema.table_exist("orders"));

        let table = schema.table("orders").await.unwrap().unwrap();
        assert_eq!(table.schema().field(0).name(), "resolved");
        assert_eq!(
            *resolver.calls.lock().unwrap(),
            vec![("public".to_owned(), "orders".to_owned())]
        );
    }

    #[tokio::test]
    async fn name_listing_feeds_the_resolver() {
        let listing = NameListing::new(BTreeMap::from([(
            "public".to_owned(),
            vec!["events".to_owned(), "users".to_owned()],
        )]));
        assert!(
            listing
                .schema("public")
                .unwrap()
                .table("events")
                .await
                .unwrap()
                .is_none()
        );

        let resolver = Arc::new(RecordingResolver::default());
        let catalog = FederatedCatalog::new(Arc::new(listing), resolver.clone());
        assert_eq!(catalog.schema_names(), vec!["public"]);
        let schema = catalog.schema("public").unwrap();
        assert_eq!(schema.table_names(), vec!["events", "users"]);
        assert!(schema.table("users").await.unwrap().is_some());
        assert!(schema.table("ghost").await.unwrap().is_none());
        assert_eq!(resolver.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn quoted_names_resolve_without_regard_to_case() {
        let resolver = Arc::new(RecordingResolver::default());
        let catalog = FederatedCatalog::new(listing(), resolver.clone());
        let schema = catalog.schema("Public").unwrap();
        assert!(schema.table_exist("Orders"));
        assert!(schema.table("ORDERS").await.unwrap().is_some());
        assert_eq!(
            *resolver.calls.lock().unwrap(),
            vec![("public".to_owned(), "orders".to_owned())]
        );
        // A name listed with capitals matches exactly; its lower case does not
        // exist and is not invented.
        let listing = NameListing::new(BTreeMap::from([("db".to_owned(), vec!["Orders".to_owned()])]));
        let catalog = FederatedCatalog::new(Arc::new(listing), resolver.clone());
        let schema = catalog.schema("db").unwrap();
        assert!(schema.table_exist("Orders"));
        assert!(!schema.table_exist("orders"));
    }

    /// A source whose names a test changes; counts its readings.
    #[derive(Debug, Default)]
    struct ChangingSource {
        names: Mutex<BTreeMap<String, Vec<String>>>,
        lists: AtomicUsize,
        finds: Mutex<Vec<(String, String)>>,
        down: AtomicBool,
    }

    impl ChangingSource {
        fn with(schemas: &[(&str, &[&str])]) -> Arc<Self> {
            let source = Self::default();
            for (schema, tables) in schemas {
                source.create(schema, tables);
            }
            Arc::new(source)
        }

        fn create(&self, schema: &str, tables: &[&str]) {
            self.names
                .lock()
                .unwrap()
                .entry(schema.to_owned())
                .or_default()
                .extend(tables.iter().map(|t| (*t).to_owned()));
        }

        fn lists(&self) -> usize {
            self.lists.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl NameSource for ChangingSource {
        async fn list(&self) -> Result<BTreeMap<String, Vec<String>>> {
            if self.down.load(Ordering::SeqCst) {
                return Err(datafusion::error::DataFusionError::Execution(
                    "source down".into(),
                ));
            }
            self.lists.fetch_add(1, Ordering::SeqCst);
            Ok(self.names.lock().unwrap().clone())
        }

        async fn find(&self, schema: &str, table: &str) -> Result<Option<String>> {
            self.finds
                .lock()
                .unwrap()
                .push((schema.to_owned(), table.to_owned()));
            let names = self.names.lock().unwrap();
            let tables = names.get(schema).cloned().unwrap_or_default();
            let lower = table.to_lowercase();
            Ok([table.to_owned(), lower]
                .into_iter()
                .find(|name| tables.contains(name)))
        }
    }

    async fn live(source: &Arc<ChangingSource>, ttl: Duration) -> (FederatedCatalog, Arc<RecordingResolver>) {
        let listing = LiveListing::read(source.clone(), ttl).await.unwrap();
        let resolver = Arc::new(RecordingResolver::default());
        (FederatedCatalog::live(listing, resolver.clone()), resolver)
    }

    #[tokio::test]
    async fn a_table_created_after_the_listing_is_found_when_named() {
        let source = ChangingSource::with(&[("public", &["orders"])]);
        let (catalog, resolver) = live(&source, Duration::from_secs(3600)).await;
        let schema = catalog.schema("public").unwrap();
        assert_eq!(schema.table_names(), vec!["orders"]);

        source.create("public", &["late_view"]);
        assert!(!schema.table_exist("late_view"), "not listed until named");
        assert!(schema.table("LATE_VIEW").await.unwrap().is_some());
        assert_eq!(
            *resolver.calls.lock().unwrap(),
            vec![("public".to_owned(), "late_view".to_owned())]
        );
        // Listed from then on, without reading the whole listing again.
        assert_eq!(schema.table_names(), vec!["late_view", "orders"]);
        assert!(catalog.schema("public").unwrap().table_exist("late_view"));
        assert_eq!(source.lists(), 1);

        // A name the source does not have is asked for and not resolved; a
        // listed one is not asked for.
        assert!(schema.table("ghost").await.unwrap().is_none());
        assert!(schema.table("orders").await.unwrap().is_some());
        assert_eq!(
            *source.finds.lock().unwrap(),
            vec![
                ("public".to_owned(), "LATE_VIEW".to_owned()),
                ("public".to_owned(), "ghost".to_owned())
            ]
        );
        assert_eq!(resolver.calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_listing_is_read_again_once_older_than_its_time_to_live() {
        let source = ChangingSource::with(&[("public", &["orders"])]);
        let (fresh, _) = live(&source, Duration::from_secs(3600)).await;
        source.create("sales", &["invoices"]);
        fresh.refresh().await.unwrap();
        assert_eq!(fresh.schema_names(), vec!["public"], "within its time to live");
        assert_eq!(source.lists(), 2 - 1);

        let (stale, _) = live(&source, Duration::ZERO).await;
        source.create("public", &["customers"]);
        source.create("hr", &[]);
        stale.refresh().await.unwrap();
        assert_eq!(stale.schema_names(), vec!["hr", "public", "sales"]);
        assert_eq!(
            stale.schema("public").unwrap().table_names(),
            vec!["customers", "orders"]
        );
        assert!(stale.schema("hr").unwrap().table_names().is_empty());
        assert_eq!(source.lists(), 3);

        // A source that cannot be listed keeps the names read before.
        source.down.store(true, Ordering::SeqCst);
        assert!(stale.refresh().await.is_err());
        assert_eq!(stale.schema_names(), vec!["hr", "public", "sales"]);

        // A fixed listing has nothing to read.
        let fixed = FederatedCatalog::new(listing(), Arc::new(RecordingResolver::default()));
        fixed.refresh().await.unwrap();
    }

    #[tokio::test]
    async fn a_schema_created_after_the_listing_is_listed_on_the_next_reading() {
        let source = ChangingSource::with(&[("public", &["orders"])]);
        let (catalog, resolver) = live(&source, Duration::ZERO).await;
        source.create("sales", &["invoices"]);

        // Unknown when named: its lookup reads the listing again on its own task.
        assert!(catalog.schema("sales").is_none());
        for _ in 0..200 {
            if catalog.schema("sales").is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let schema = catalog.schema("Sales").expect("listed after the reading");
        assert!(schema.table("invoices").await.unwrap().is_some());
        assert_eq!(
            *resolver.calls.lock().unwrap(),
            vec![("sales".to_owned(), "invoices".to_owned())]
        );
    }

    #[tokio::test]
    async fn an_unknown_schema_is_not_read_again_within_the_time_to_live() {
        let source = ChangingSource::with(&[("public", &["orders"])]);
        let (catalog, _) = live(&source, Duration::from_secs(3600)).await;
        source.create("sales", &["invoices"]);
        assert!(catalog.schema("sales").is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(catalog.schema("sales").is_none());
        assert_eq!(source.lists(), 1);
    }

    #[test]
    fn without_a_runtime_an_unknown_schema_stays_unknown() {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let source = ChangingSource::with(&[("public", &[])]);
        let (catalog, _) = runtime.block_on(live(&source, Duration::ZERO));
        source.create("sales", &[]);
        assert!(catalog.schema("sales").is_none());
        assert_eq!(source.lists(), 1);
    }

    #[tokio::test]
    async fn one_reading_at_a_time_in_the_background() {
        let source = ChangingSource::with(&[("public", &[])]);
        let (catalog, _) = live(&source, Duration::ZERO).await;
        // Two lookups before the task runs (a current-thread runtime): one reading.
        assert!(catalog.schema("a").is_none());
        assert!(catalog.schema("b").is_none());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(source.lists(), 2);
        // Once it ended, another may start.
        assert!(catalog.schema("c").is_none());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(source.lists(), 3);
    }

    #[tokio::test]
    async fn unknown_tables_never_reach_the_source() {
        let resolver = Arc::new(RecordingResolver::default());
        let catalog = FederatedCatalog::new(listing(), resolver.clone());
        let schema = catalog.schema("public").unwrap();

        assert!(schema.table("missing").await.unwrap().is_none());
        assert!(!schema.table_exist("missing"));
        assert!(catalog.schema("nope").is_none());
        assert!(resolver.calls.lock().unwrap().is_empty());
    }
}
