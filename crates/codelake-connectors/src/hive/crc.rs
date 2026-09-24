//! Hadoop `ChecksumFileSystem` side files (`.<name>.crc`).
//!
//! Trino's file metastore writes through Hadoop's local file system, which keeps
//! a checksum next to every file and verifies it on read when present. A
//! metadata file rewritten without its checksum makes Trino fail, which would
//! break the rollback path, so every metadata write also writes its `.crc`.
//!
//! Format: `b"crc\0"`, big-endian `i32` bytes per checksum (512), then one
//! big-endian CRC-32 (IEEE) per chunk.

const BYTES_PER_CHECKSUM: usize = 512;

pub(crate) fn hadoop_crc(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + data.len().div_ceil(BYTES_PER_CHECKSUM) * 4);
    out.extend_from_slice(b"crc\0");
    out.extend_from_slice(&512_i32.to_be_bytes());
    for chunk in data.chunks(BYTES_PER_CHECKSUM) {
        out.extend_from_slice(&crc32fast::hash(chunk).to_be_bytes());
    }
    out
}

/// Name of the checksum file for `file_name` (`x` -> `.x.crc`).
pub(crate) fn crc_name(file_name: &str) -> String {
    format!(".{file_name}.crc")
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &[u8] = include_bytes!("../../tests/fixtures/trino431/warehouse/pilot/t1/.trinoSchema");
    const TABLE_CRC: &[u8] =
        include_bytes!("../../tests/fixtures/trino431/warehouse/pilot/t1/..trinoSchema.crc");
    const SCHEMA: &[u8] = include_bytes!("../../tests/fixtures/trino431/warehouse/.pilot.trinoSchema");
    const SCHEMA_CRC: &[u8] =
        include_bytes!("../../tests/fixtures/trino431/warehouse/..pilot.trinoSchema.crc");

    #[test]
    fn matches_checksums_written_by_trino() {
        assert_eq!(hadoop_crc(TABLE), TABLE_CRC);
        assert_eq!(hadoop_crc(SCHEMA), SCHEMA_CRC);
    }

    #[test]
    fn empty_and_exact_chunk_sizes() {
        assert_eq!(hadoop_crc(b""), b"crc\0\0\0\x02\0".to_vec());
        assert_eq!(hadoop_crc(&[7_u8; 512]).len(), 12);
        assert_eq!(hadoop_crc(&[7_u8; 513]).len(), 16);
    }

    #[test]
    fn checksum_file_names() {
        assert_eq!(crc_name(".trinoSchema"), "..trinoSchema.crc");
        assert_eq!(crc_name("user_dbt"), ".user_dbt.crc");
    }
}
