output "namespace" {
  value = kubernetes_namespace_v1.temporalstore.metadata[0].name
}

output "proxy_service_name" {
  value = kubernetes_service_v1.proxy.metadata[0].name
}

output "redis_service_name" {
  value = kubernetes_service_v1.redis_proxy.metadata[0].name
}

output "proxy_internal_addr" {
  value = "${local.proxy_service_name}.${var.namespace}.svc.cluster.local:17000"
}

output "redis_internal_addr" {
  value = "${local.redis_service_name}.${var.namespace}.svc.cluster.local:16379"
}

output "validation_job_names" {
  value = var.enable_validation_jobs ? {
    for key, job in kubernetes_job_v1.validation : key => job.metadata[0].name
  } : {}
}
