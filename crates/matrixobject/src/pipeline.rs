use crate::types::*;
use async_trait::async_trait;
use bytes::Bytes;
use crc32fast::Hasher;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct IoAttrs {
    values: BTreeMap<String, String>,
}

impl IoAttrs {
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.values.insert(key.into(), value.into());
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }
}

#[derive(Debug, Clone)]
pub struct PipelineWrite {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub data: Bytes,
    pub attrs: IoAttrs,
}

#[derive(Debug, Clone)]
pub struct PipelineRead {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub length: u64,
    pub attrs: IoAttrs,
}

#[derive(Debug, Clone)]
pub struct PipelineBackendInfo {
    pub name: String,
    pub attrs: IoAttrs,
}

#[async_trait]
pub trait PipelineStage: Send + Sync {
    fn name(&self) -> &'static str;
    async fn write(&self, req: PipelineWrite) -> Result<PipelineWrite>;
    async fn read(&self, req: PipelineRead) -> Result<PipelineRead>;
    async fn backend_info(&self, info: PipelineBackendInfo) -> Result<PipelineBackendInfo> {
        Ok(info)
    }
}

#[derive(Clone, Default)]
pub struct MatrixObjectPipeline {
    stages: Arc<Vec<Arc<dyn PipelineStage>>>,
}

impl fmt::Debug for MatrixObjectPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stages: Vec<&'static str> = self.stages.iter().map(|stage| stage.name()).collect();
        f.debug_struct("MatrixObjectPipeline")
            .field("stages", &stages)
            .finish()
    }
}

impl MatrixObjectPipeline {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn byte_store_default() -> Self {
        Self::byte_store_with_compression(CompressionKind::None)
    }

    pub fn byte_store_with_compression(compression: CompressionKind) -> Self {
        Self::new(vec![
            Arc::new(ChecksumStage) as Arc<dyn PipelineStage>,
            Arc::new(CompressionStage::new(compression)),
            Arc::new(ErasureCodingStage::passthrough()),
        ])
    }

    pub fn new(stages: Vec<Arc<dyn PipelineStage>>) -> Self {
        Self {
            stages: Arc::new(stages),
        }
    }

    pub fn push_stage(&self, stage: Arc<dyn PipelineStage>) -> Self {
        let mut stages = self.stages.as_ref().clone();
        stages.push(stage);
        Self::new(stages)
    }

    pub async fn prepare_write(&self, mut req: PipelineWrite) -> Result<PipelineWrite> {
        for stage in self.stages.iter() {
            req = stage
                .write(req)
                .await
                .map_err(|err| MatrixObjectError::Pipeline {
                    stage: stage.name().to_string(),
                    message: err.to_string(),
                })?;
        }
        Ok(req)
    }

    pub async fn prepare_read(&self, mut req: PipelineRead) -> Result<PipelineRead> {
        for stage in self.stages.iter().rev() {
            req = stage
                .read(req)
                .await
                .map_err(|err| MatrixObjectError::Pipeline {
                    stage: stage.name().to_string(),
                    message: err.to_string(),
                })?;
        }
        Ok(req)
    }

    pub async fn backend_info(&self) -> Result<PipelineBackendInfo> {
        let mut info = PipelineBackendInfo {
            name: "matrixobject".to_string(),
            attrs: IoAttrs::default(),
        };
        for stage in self.stages.iter() {
            info = stage.backend_info(info).await?;
        }
        Ok(info)
    }
}

#[derive(Debug)]
pub struct ChecksumStage;

#[async_trait]
impl PipelineStage for ChecksumStage {
    fn name(&self) -> &'static str {
        "checksum"
    }

    async fn write(&self, mut req: PipelineWrite) -> Result<PipelineWrite> {
        req.attrs.insert("crc32", crc32_hex(&req.data));
        Ok(req)
    }

    async fn read(&self, req: PipelineRead) -> Result<PipelineRead> {
        Ok(req)
    }

    async fn backend_info(&self, mut info: PipelineBackendInfo) -> Result<PipelineBackendInfo> {
        info.attrs.insert("checksum", "crc32");
        Ok(info)
    }
}

#[derive(Debug, Clone)]
pub struct CompressionStage {
    kind: CompressionKind,
}

impl CompressionStage {
    pub fn new(kind: CompressionKind) -> Self {
        Self { kind }
    }

    pub fn passthrough() -> Self {
        Self {
            kind: CompressionKind::None,
        }
    }
}

#[async_trait]
impl PipelineStage for CompressionStage {
    fn name(&self) -> &'static str {
        "compression"
    }

    async fn write(&self, mut req: PipelineWrite) -> Result<PipelineWrite> {
        req.attrs.insert("compression", format!("{:?}", self.kind));
        Ok(req)
    }

    async fn read(&self, req: PipelineRead) -> Result<PipelineRead> {
        Ok(req)
    }

    async fn backend_info(&self, mut info: PipelineBackendInfo) -> Result<PipelineBackendInfo> {
        info.attrs.insert("compression", format!("{:?}", self.kind));
        Ok(info)
    }
}

pub fn encode_compressed(kind: CompressionKind, data: Bytes) -> Result<Bytes> {
    match kind {
        CompressionKind::None => Ok(data),
        CompressionKind::Zstd => {
            let encoded =
                zstd::stream::encode_all(data.as_ref(), 0).map_err(MatrixObjectError::Io)?;
            Ok(Bytes::from(encoded))
        }
        CompressionKind::Lz4 => Ok(Bytes::from(lz4_flex::compress_prepend_size(data.as_ref()))),
    }
}

pub fn decode_compressed(kind: CompressionKind, data: Bytes) -> Result<Bytes> {
    match kind {
        CompressionKind::None => Ok(data),
        CompressionKind::Zstd => {
            let decoded = zstd::stream::decode_all(data.as_ref()).map_err(MatrixObjectError::Io)?;
            Ok(Bytes::from(decoded))
        }
        CompressionKind::Lz4 => lz4_flex::decompress_size_prepended(data.as_ref())
            .map(Bytes::from)
            .map_err(|err| MatrixObjectError::Pipeline {
                stage: "compression".to_owned(),
                message: err.to_string(),
            }),
    }
}

#[derive(Debug, Clone)]
pub struct ErasureCodingStage {
    enabled: bool,
}

impl ErasureCodingStage {
    pub fn passthrough() -> Self {
        Self { enabled: false }
    }
}

#[async_trait]
impl PipelineStage for ErasureCodingStage {
    fn name(&self) -> &'static str {
        "erasure_coding"
    }

    async fn write(&self, mut req: PipelineWrite) -> Result<PipelineWrite> {
        req.attrs.insert("erasure_coding", self.enabled.to_string());
        Ok(req)
    }

    async fn read(&self, req: PipelineRead) -> Result<PipelineRead> {
        Ok(req)
    }

    async fn backend_info(&self, mut info: PipelineBackendInfo) -> Result<PipelineBackendInfo> {
        info.attrs
            .insert("erasure_coding", self.enabled.to_string());
        Ok(info)
    }
}

fn crc32_hex(bytes: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    format!("{:08x}", hasher.finalize())
}
