use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir};
use temporalstore_snapshot::{
    FileObjectStore, LocalSnapshot, ObjectStore, ObjectStoreError, S3SnapshotStore, SnapshotStore,
};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug)]
struct AwsCliObjectStore {
    bucket: String,
    prefix: String,
}

impl AwsCliObjectStore {
    fn from_env() -> Option<Self> {
        let bucket = std::env::var("TS_SNAPSHOT_AWS_BUCKET").ok()?;
        let prefix = std::env::var("TS_SNAPSHOT_AWS_PREFIX").unwrap_or_else(|_| {
            format!("temporalstore-rust-snapshot-smoke/{}", uuid::Uuid::new_v4())
        });
        Some(Self {
            bucket,
            prefix: prefix.trim_matches('/').to_string(),
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    fn s3_uri(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, self.full_key(key))
    }

    fn run_aws(&self, args: &[&str]) -> Result<String, ObjectStoreError> {
        let output = Command::new("aws")
            .args(args)
            .output()
            .map_err(ObjectStoreError::Io)?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(ObjectStoreError::InvalidKey(format!(
                "aws {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            )))
        }
    }
}

#[async_trait]
impl ObjectStore for AwsCliObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let mut tmp = NamedTempFile::new().map_err(ObjectStoreError::Io)?;
        std::io::Write::write_all(&mut tmp, &bytes).map_err(ObjectStoreError::Io)?;
        let path = tmp.path().to_string_lossy().to_string();
        self.run_aws(&["s3", "cp", &path, &self.s3_uri(key)])?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let tmp = NamedTempFile::new().map_err(ObjectStoreError::Io)?;
        let path = tmp.path().to_string_lossy().to_string();
        self.run_aws(&["s3", "cp", &self.s3_uri(key), &path])?;
        Ok(Bytes::from(
            std::fs::read(path).map_err(ObjectStoreError::Io)?,
        ))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let full_prefix = self.full_key(prefix);
        let out = self.run_aws(&[
            "s3api",
            "list-objects-v2",
            "--bucket",
            &self.bucket,
            "--prefix",
            &full_prefix,
            "--query",
            "Contents[].Key",
            "--output",
            "json",
        ])?;
        let keys: Vec<String> = serde_json::from_str(&out).map_err(|err| {
            ObjectStoreError::InvalidKey(format!("failed to parse aws list output: {err}"))
        })?;
        let strip = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        Ok(keys
            .into_iter()
            .filter_map(|key| key.strip_prefix(&strip).map(str::to_string))
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.run_aws(&["s3", "rm", &self.s3_uri(key)])?;
        Ok(())
    }

    fn uri(&self, key: &str) -> String {
        self.s3_uri(key)
    }
}

async fn sample_snapshot(root: &Path) -> LocalSnapshot {
    let shard_root = root.join("shard-101");
    tokio::fs::create_dir_all(shard_root.join("page_segments"))
        .await
        .unwrap();
    let index_path = shard_root.join("index.bin");
    let checksums_path = shard_root.join("checksums.json");
    let segment_path = shard_root.join("page_segments/0001.seg");
    tokio::fs::write(&index_path, b"aws-index").await.unwrap();
    tokio::fs::write(&checksums_path, b"[]").await.unwrap();
    tokio::fs::write(&segment_path, b"aws-page-segment")
        .await
        .unwrap();

    let file_store = Arc::new(FileObjectStore::new(root.join("bootstrap-objects")));
    let snapshot_store = S3SnapshotStore::new("aws-smoke-cluster", "test", root, file_store);
    snapshot_store
        .create_local_snapshot(101, "term:3:index:44".to_string())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires TS_SNAPSHOT_AWS_BUCKET and AWS CLI credentials"]
async fn aws_s3_snapshot_round_trip() {
    let Some(store) = AwsCliObjectStore::from_env() else {
        eprintln!("set TS_SNAPSHOT_AWS_BUCKET to run this AWS S3 smoke test");
        return;
    };
    let tmp = TempDir::new().unwrap();
    let snapshot_store = S3SnapshotStore::new(
        "aws-smoke-cluster",
        "test",
        tmp.path().join("local"),
        Arc::new(store),
    );

    let snapshot = sample_snapshot(tmp.path()).await;
    let snapshot_ref = snapshot_store.upload_snapshot(snapshot).await.unwrap();
    snapshot_store.verify_snapshot(&snapshot_ref).await.unwrap();
    let restored = snapshot_store
        .download_snapshot(&snapshot_ref, PathBuf::from(tmp.path()).join("restore"))
        .await
        .unwrap();
    let mut restored_segment =
        tokio::fs::File::open(restored.root_dir.join("page_segments/0001.seg"))
            .await
            .unwrap();
    let mut bytes = Vec::new();
    restored_segment.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"aws-page-segment");
    snapshot_store.delete_snapshot(&snapshot_ref).await.unwrap();
}
