// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::types::Command;

pub(super) fn proxy_command_is_write(command: &Command) -> bool {
    // Delegate to the engine's write-command classifier -- the same one that gates WAL
    // persistence, and the one the client and the data node already delegate to after a
    // hand-maintained subset drifted in each of them.
    //
    // This copy had drifted too, by fourteen commands: every list and sorted-set mutation
    // (ListPush/ListPop, ZSetAdd/ZSetRemove/ZSetIncrBy/ZSetPop), BucketTake, SeenCheck,
    // CommonPersist and the whole resource-blob upload path. Both things this answers for
    // were wrong as a result -- a proxy in Readonly or WriteDisabled forwarded those writes
    // instead of refusing them, and the write inflight quota never counted them.
    crate::engine::is_write_command(command)
}

pub(super) fn proxy_command_routing_key(command: &Command) -> Option<String> {
    // Delegate to the client's routing-key extractor rather than keeping a copy.
    //
    // The copy that used to live here keyed the same 61 plain commands as the client's, and
    // differed on exactly one context command: it did not key ContextSetNodeEmbedding at all,
    // so a node the drain was shedding was refused when read and accepted when its embedding
    // was written. Fixing that instance left the copy in place to drift again.
    //
    // The key STRING this now returns differs from the old one for context commands, and that
    // is fine: the only consumer hashes it to decide whether to shed, and all that requires is
    // that the decision be the same on every attempt for the same command. It also means the
    // proxy and the client now shed the same keys rather than two unrelated subsets.
    crate::client::command_routing_key(command)
}
