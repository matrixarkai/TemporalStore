use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use matrixobjectstore_rs::{MatrixObjectStore, ObjectError as MatrixObjectError, StoreOptions};
use temporalstore_snapshot::object_store::{ObjectStore, ObjectStoreError};

#[derive(Debug, Clone)]
pub struct MatrixObjectObjectStore {
    bucket: String,
    content_type: String,
    inner: Arc<Mutex<MatrixObjectStore>>,
}

impl MatrixObjectObjectStore {
    pub fn new(bucket: impl Into<String>, options: StoreOptions) -> Result<Self, ObjectStoreError> {
        let bucket = bucket.into();
        let mut inner = MatrixObjectStore::new(options).map_err(map_matrixobject_error)?;
        inner
            .create_bucket(&bucket)
            .map_err(map_matrixobject_error)?;
        Ok(Self {
            bucket,
            content_type: "application/octet-stream".to_string(),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn with_default_options(bucket: impl Into<String>) -> Result<Self, ObjectStoreError> {
        Self::new(bucket, StoreOptions::default())
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn inner(&self) -> Arc<Mutex<MatrixObjectStore>> {
        Arc::clone(&self.inner)
    }
}

#[async_trait]
impl ObjectStore for MatrixObjectObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let mut inner = self.inner.lock().expect("matrixobject lock poisoned");
        inner
            .put_object(&self.bucket, key, bytes.to_vec(), self.content_type.clone())
            .map_err(map_matrixobject_error)?;
        Ok(())
    }

    async fn append_blob(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
        let mut inner = self.inner.lock().expect("matrixobject lock poisoned");
        inner
            .append_object(&self.bucket, key, bytes.to_vec(), self.content_type.clone())
            .map_err(map_matrixobject_error)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let inner = self.inner.lock().expect("matrixobject lock poisoned");
        let object = inner
            .get_object(&self.bucket, key)
            .map_err(map_matrixobject_error)?;
        Ok(Bytes::from(object.data))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let inner = self.inner.lock().expect("matrixobject lock poisoned");
        let mut marker = None;
        let mut out = Vec::new();
        let limit = 1024;
        loop {
            let objects = inner
                .list_objects(&self.bucket, prefix, marker.as_deref(), limit)
                .map_err(map_matrixobject_error)?;
            if objects.is_empty() {
                break;
            }
            let page_len = objects.len();
            marker = objects
                .last()
                .map(|metadata| metadata.object_id.key.clone());
            out.extend(objects.into_iter().map(|metadata| metadata.object_id.key));
            if page_len < limit {
                break;
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let mut inner = self.inner.lock().expect("matrixobject lock poisoned");
        match inner.delete_object(&self.bucket, key) {
            Ok(_) | Err(MatrixObjectError::NotFound(_)) => Ok(()),
            Err(err) => Err(map_matrixobject_error(err)),
        }
    }

    fn uri(&self, key: &str) -> String {
        format!("matrixobject://{}/{}", self.bucket, key)
    }
}

fn map_matrixobject_error(err: MatrixObjectError) -> ObjectStoreError {
    match err {
        MatrixObjectError::NotFound(message) => ObjectStoreError::NotFound(message),
        MatrixObjectError::InvalidArgument(message) => ObjectStoreError::InvalidKey(message),
        MatrixObjectError::PreconditionFailed(message) | MatrixObjectError::Corruption(message) => {
            ObjectStoreError::InvalidKey(message)
        }
    }
}
