variable "aws_region" {
  description = "AWS region containing the existing EKS cluster."
  type        = string
}

variable "aws_profile" {
  description = "Optional AWS CLI profile to use."
  type        = string
  default     = ""
}

variable "eks_cluster_name" {
  description = "Name of an existing EKS cluster. Terraform reuses this cluster and does not create a new one."
  type        = string
}

variable "namespace" {
  description = "Kubernetes namespace for TemporalStore."
  type        = string
  default     = "temporalstore"
}

variable "image" {
  description = "Container image containing metaserver, server, proxy, and redis_proxy release binaries."
  type        = string
}

variable "image_pull_policy" {
  description = "Kubernetes image pull policy."
  type        = string
  default     = "IfNotPresent"
}

variable "shard_id" {
  description = "Single-node shard id."
  type        = number
  default     = 1
}

variable "storage_class_name" {
  description = "Existing Kubernetes StorageClass for the server PVC. Empty uses the cluster default."
  type        = string
  default     = ""
}

variable "server_storage_size" {
  description = "PVC size for local cache, page store, and index data."
  type        = string
  default     = "20Gi"
}

variable "cache_memory_bytes" {
  description = "L1 memory cache size for temporalstore server."
  type        = number
  default     = 16777216
}

variable "proxy_replicas" {
  description = "Replica count for the stateless HTTP proxy."
  type        = number
  default     = 1
}

variable "redis_proxy_replicas" {
  description = "Replica count for the stateless Redis-compatible proxy."
  type        = number
  default     = 1
}

variable "proxy_service_type" {
  description = "Service type for the HTTP proxy."
  type        = string
  default     = "LoadBalancer"
}

variable "redis_service_type" {
  description = "Service type for the Redis-compatible proxy."
  type        = string
  default     = "LoadBalancer"
}

variable "labels" {
  description = "Extra labels applied to Kubernetes resources."
  type        = map(string)
  default     = {}
}

variable "enable_validation_jobs" {
  description = "Create one-shot EKS jobs that run TemporalStore Rust raft, storage, and QPS validation harnesses."
  type        = bool
  default     = false
}

variable "validation_backoff_limit" {
  description = "Backoff limit for validation jobs."
  type        = number
  default     = 0
}

variable "validation_ttl_seconds_after_finished" {
  description = "TTL for completed validation jobs. Set null to keep jobs."
  type        = number
  default     = 3600
}

variable "raft_validation_args" {
  description = "Arguments passed to distributed_raft_harness."
  type        = list(string)
  default     = []
}

variable "scale_validation_args" {
  description = "Arguments passed to scale_harness for write/read QPS, latency, failover, and shared-store comparison."
  type        = list(string)
  default = [
    "--nodes", "5",
    "--string-ops", "5000",
    "--hash-ops", "1000",
    "--sequence-keys", "8",
    "--sequence-len", "1000",
    "--scale-events", "4",
    "--failover-every", "500",
    "--read-sample-every", "50",
    "--compare-shared-store", "true",
    "--shared-store-ops", "5000",
    "--shared-store-flush-every", "50",
  ]
}

variable "storage_validation_args" {
  description = "Arguments passed to storage_modes_harness."
  type        = list(string)
  default     = []
}
