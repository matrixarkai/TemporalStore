// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SingleNodeMeta server/proxy/table state setters, extracted from meta.rs.

use super::*;

impl SingleNodeMeta {
    pub(super) fn set_server_state(&self, request: StateChangeRequest, next: MetaEntityState) -> AckResponse {
        if !self
            .inner
            .read()
            .expect("meta lock poisoned")
            .servers
            .contains_key(&request.endpoint)
        {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        }
        match next {
            MetaEntityState::Frozen => {
                self.record_mutation(MetaMutation::FreezeServer(request.clone()));
            }
            MetaEntityState::Dropped => {
                self.record_mutation(MetaMutation::DropServer(request.clone()));
            }
            MetaEntityState::Normal => {}
        }
        self.apply_set_server_state(request, next)
    }

    pub(super) fn apply_set_server_state(
        &self,
        request: StateChangeRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(server) = state.servers.get_mut(&request.endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        };
        let now = now_ms();
        server.state = next;
        match next {
            MetaEntityState::Frozen => {
                server.frozen_since_ms = now;
                server.freeze_cooldown_until_ms = now.saturating_add(request.freeze_cooldown_ms);
            }
            MetaEntityState::Normal => {
                server.frozen_since_ms = 0;
                server.freeze_cooldown_until_ms = 0;
            }
            MetaEntityState::Dropped => {}
        }
        stamp_dropped_since(&mut state, &dropped_key("server", &request.endpoint), next, now);
        record_topology_event(
            &mut state,
            "server_state",
            format!("server:{}", request.endpoint),
            format!("state={}", next.as_str()),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub(super) fn set_proxy_state(&self, request: StateChangeRequest, next: MetaEntityState) -> AckResponse {
        if !self
            .inner
            .read()
            .expect("meta lock poisoned")
            .proxies
            .contains_key(&request.endpoint)
        {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        }
        match next {
            MetaEntityState::Frozen => {
                self.record_mutation(MetaMutation::FreezeProxy(request.clone()));
            }
            MetaEntityState::Dropped => {
                self.record_mutation(MetaMutation::DropProxy(request.clone()));
            }
            MetaEntityState::Normal => {}
        }
        self.apply_set_proxy_state(request, next)
    }

    pub(super) fn apply_set_proxy_state(
        &self,
        request: StateChangeRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(proxy) = state.proxies.get_mut(&request.endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        };
        let now = now_ms();
        proxy.state = next;
        match next {
            MetaEntityState::Frozen => {
                proxy.frozen_since_ms = now;
                proxy.freeze_cooldown_until_ms = now.saturating_add(request.freeze_cooldown_ms);
            }
            MetaEntityState::Normal => {
                proxy.frozen_since_ms = 0;
                proxy.freeze_cooldown_until_ms = 0;
            }
            MetaEntityState::Dropped => {}
        }
        stamp_dropped_since(&mut state, &dropped_key("proxy", &request.endpoint), next, now);
        record_topology_event(
            &mut state,
            "proxy_state",
            format!("proxy:{}", request.endpoint),
            format!("state={}", next.as_str()),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub(super) fn apply_set_table_state(
        &self,
        request: DeleteTableRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        let key = table_key(&request.namespace, &request.table_name);
        let Some(existing) = state.tables.get(&key).map(|table| table.info.state) else {
            return AckResponse {
                status: Status::error("table_not_found", "table not found"),
            };
        };
        if existing == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("table_not_found", "table is dropped"),
            };
        }
        if existing == next {
            return AckResponse {
                status: Status::error("not_modified", "table state is unchanged"),
            };
        }
        let topology_version = record_topology_event(
            &mut state,
            "table_state",
            format!("table:{}/{}", request.namespace, request.table_name),
            format!("state={}", next.as_str()),
        );
        let table = state
            .tables
            .get_mut(&key)
            .expect("table exists after state validation");
        table.info.state = next;
        table.info.topology_version = topology_version;
        stamp_dropped_since(&mut state, &dropped_key("table", &key), next, now_ms());
        AckResponse {
            status: Status::ok(),
        }
    }
}
