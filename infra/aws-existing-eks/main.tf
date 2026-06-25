locals {
  labels = merge(
    {
      "app.kubernetes.io/name"       = "temporalstore"
      "app.kubernetes.io/managed-by" = "terraform"
    },
    var.labels,
  )

  meta_service_name   = "temporalstore-metaserver"
  server_service_name = "temporalstore-server"
  proxy_service_name  = "temporalstore-proxy"
  redis_service_name  = "temporalstore-redis"

  meta_addr             = "${local.meta_service_name}.${var.namespace}.svc.cluster.local:17001"
  server_advertise_addr = "${local.server_service_name}.${var.namespace}.svc.cluster.local:17002"
  proxy_addr            = "${local.proxy_service_name}.${var.namespace}.svc.cluster.local:17000"

  validation_jobs = {
    raft = {
      name    = "temporalstore-raft-validation"
      command = ["/usr/local/bin/distributed_raft_harness"]
      args    = var.raft_validation_args
    }
    scale = {
      name    = "temporalstore-scale-validation"
      command = ["/usr/local/bin/scale_harness"]
      args    = var.scale_validation_args
    }
    storage = {
      name    = "temporalstore-storage-validation"
      command = ["/usr/local/bin/storage_modes_harness"]
      args    = var.storage_validation_args
    }
  }
}

resource "kubernetes_namespace_v1" "temporalstore" {
  metadata {
    name   = var.namespace
    labels = local.labels
  }
}

resource "kubernetes_persistent_volume_claim_v1" "server_data" {
  metadata {
    name      = "temporalstore-server-data"
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = local.labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class_name == "" ? null : var.storage_class_name
    resources {
      requests = {
        storage = var.server_storage_size
      }
    }
  }
}

resource "kubernetes_deployment_v1" "metaserver" {
  metadata {
    name      = local.meta_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "metaserver" })
  }

  spec {
    replicas = 1
    selector {
      match_labels = { app = local.meta_service_name }
    }
    template {
      metadata {
        labels = merge(local.labels, { app = local.meta_service_name, "app.kubernetes.io/component" = "metaserver" })
      }
      spec {
        container {
          name              = "metaserver"
          image             = var.image
          image_pull_policy = var.image_pull_policy
          command           = ["/usr/local/bin/metaserver"]
          env {
            name  = "TS_META_BIND_ADDR"
            value = "0.0.0.0:17001"
          }
          port {
            name           = "http"
            container_port = 17001
          }
          readiness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
          liveness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "metaserver" {
  metadata {
    name      = local.meta_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "metaserver" })
  }
  spec {
    selector = { app = local.meta_service_name }
    port {
      name        = "http"
      port        = 17001
      target_port = "http"
    }
  }
}

resource "kubernetes_deployment_v1" "server" {
  metadata {
    name      = local.server_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "server" })
  }

  spec {
    replicas = 1
    selector {
      match_labels = { app = local.server_service_name }
    }
    template {
      metadata {
        labels = merge(local.labels, { app = local.server_service_name, "app.kubernetes.io/component" = "server" })
      }
      spec {
        container {
          name              = "server"
          image             = var.image
          image_pull_policy = var.image_pull_policy
          command           = ["/usr/local/bin/server"]
          env {
            name  = "TS_SERVER_BIND_ADDR"
            value = "0.0.0.0:17002"
          }
          env {
            name  = "TS_SERVER_ADVERTISE_ADDR"
            value = local.server_advertise_addr
          }
          env {
            name  = "TS_META_ADDR"
            value = local.meta_addr
          }
          env {
            name  = "TS_SHARD_ID"
            value = tostring(var.shard_id)
          }
          env {
            name  = "TS_CACHE_MEMORY_BYTES"
            value = tostring(var.cache_memory_bytes)
          }
          env {
            name  = "TS_CACHE_DIR"
            value = "/var/lib/temporalstore/cache"
          }
          env {
            name  = "TS_PAGE_STORE_DIR"
            value = "/var/lib/temporalstore/pages"
          }
          env {
            name  = "TS_INDEX_DIR"
            value = "/var/lib/temporalstore/indexes"
          }
          port {
            name           = "http"
            container_port = 17002
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/temporalstore"
          }
          readiness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
          liveness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
        }
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.server_data.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "server" {
  metadata {
    name      = local.server_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "server" })
  }
  spec {
    selector = { app = local.server_service_name }
    port {
      name        = "http"
      port        = 17002
      target_port = "http"
    }
  }
}

resource "kubernetes_deployment_v1" "proxy" {
  metadata {
    name      = local.proxy_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "proxy" })
  }

  spec {
    replicas = var.proxy_replicas
    selector {
      match_labels = { app = local.proxy_service_name }
    }
    template {
      metadata {
        labels = merge(local.labels, { app = local.proxy_service_name, "app.kubernetes.io/component" = "proxy" })
      }
      spec {
        container {
          name              = "proxy"
          image             = var.image
          image_pull_policy = var.image_pull_policy
          command           = ["/usr/local/bin/proxy"]
          env {
            name  = "TS_PROXY_BIND_ADDR"
            value = "0.0.0.0:17000"
          }
          env {
            name  = "TS_META_ADDR"
            value = local.meta_addr
          }
          port {
            name           = "http"
            container_port = 17000
          }
          readiness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
          liveness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "proxy" {
  metadata {
    name      = local.proxy_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "proxy" })
  }
  spec {
    type     = var.proxy_service_type
    selector = { app = local.proxy_service_name }
    port {
      name        = "http"
      port        = 17000
      target_port = "http"
    }
  }
}

resource "kubernetes_deployment_v1" "redis_proxy" {
  metadata {
    name      = local.redis_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "redis-proxy" })
  }

  spec {
    replicas = var.redis_proxy_replicas
    selector {
      match_labels = { app = local.redis_service_name }
    }
    template {
      metadata {
        labels = merge(local.labels, { app = local.redis_service_name, "app.kubernetes.io/component" = "redis-proxy" })
      }
      spec {
        container {
          name              = "redis-proxy"
          image             = var.image
          image_pull_policy = var.image_pull_policy
          command           = ["/usr/local/bin/redis_proxy"]
          env {
            name  = "TS_REDIS_BIND_ADDR"
            value = "0.0.0.0:16379"
          }
          env {
            name  = "TS_PROXY_ADDR"
            value = local.proxy_addr
          }
          env {
            name  = "TS_SHARD_ID"
            value = tostring(var.shard_id)
          }
          port {
            name           = "resp"
            container_port = 16379
          }
          readiness_probe {
            tcp_socket {
              port = "resp"
            }
          }
          liveness_probe {
            tcp_socket {
              port = "resp"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "redis_proxy" {
  metadata {
    name      = local.redis_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "redis-proxy" })
  }
  spec {
    type     = var.redis_service_type
    selector = { app = local.redis_service_name }
    port {
      name        = "resp"
      port        = 16379
      target_port = "resp"
    }
  }
}

resource "kubernetes_job_v1" "validation" {
  for_each = var.enable_validation_jobs ? local.validation_jobs : {}

  metadata {
    name      = each.value.name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels = merge(
      local.labels,
      {
        "app.kubernetes.io/component" = "validation"
        "temporalstore.io/test"       = each.key
      },
    )
  }

  spec {
    backoff_limit              = var.validation_backoff_limit
    ttl_seconds_after_finished = var.validation_ttl_seconds_after_finished

    template {
      metadata {
        labels = merge(
          local.labels,
          {
            "app.kubernetes.io/component" = "validation"
            "temporalstore.io/test"       = each.key
          },
        )
      }

      spec {
        restart_policy = "Never"

        container {
          name              = each.key
          image             = var.image
          image_pull_policy = var.image_pull_policy
          command           = each.value.command
          args              = each.value.args

          env {
            name  = "RUST_BACKTRACE"
            value = "1"
          }
        }
      }
    }
  }

  depends_on = [
    kubernetes_deployment_v1.metaserver,
    kubernetes_deployment_v1.server,
    kubernetes_deployment_v1.proxy,
    kubernetes_deployment_v1.redis_proxy,
  ]
}
