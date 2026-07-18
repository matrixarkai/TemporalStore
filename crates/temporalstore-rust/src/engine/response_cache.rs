use crate::types::CommandResponse;

use matrixcache::{CacheKey, MultiLayerCache};

pub(super) fn cached_response(
    cache: &MultiLayerCache,
    key: CacheKey,
    source: impl FnOnce() -> CommandResponse,
) -> CommandResponse {
    if let Ok(Some(bytes)) = cache.get(&key) {
        if let Ok(response) = serde_json::from_slice::<CommandResponse>(&bytes) {
            return response;
        }
        let _ = cache.invalidate(&key);
    }
    let response = source();
    if let Ok(bytes) = serde_json::to_vec(&response) {
        cache.put_memory_only(key, bytes);
    }
    response
}

pub(super) fn invalidate_if_cached(cache: &MultiLayerCache, key: CacheKey) {
    if cache.peek(&key) {
        let _ = cache.invalidate(&key);
    }
}
