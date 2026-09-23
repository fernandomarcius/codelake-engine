//! `MongoDB` connector against a real `MongoDB`.

use std::sync::Arc;

use crate::common;
use codelake_connectors::mongodb::MongoConnector;
use mongodb::bson::doc;
use secrecy::SecretString;
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn collections_are_queryable_as_tables() {
    let container = Mongo::default().start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let uri = format!("mongodb://{host}:{port}/app?directConnection=true&tls=false");

    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    client
        .database("app")
        .collection("events")
        .insert_many(vec![
            doc! { "kind": "login", "user_id": 1_i64 },
            doc! { "kind": "login", "user_id": 2_i64 },
            doc! { "kind": "logout", "user_id": 1_i64 },
        ])
        .await
        .unwrap();

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        Arc::new(MongoConnector::new("events".into(), SecretString::from(uri))),
    )
    .await;

    let result = common::query(
        &engine,
        "SELECT kind, count(*) AS n FROM events.public.events GROUP BY kind ORDER BY kind",
    )
    .await;
    assert!(result.contains("| login  | 2"), "{result}");
    assert!(result.contains("| logout | 1"), "{result}");
}
