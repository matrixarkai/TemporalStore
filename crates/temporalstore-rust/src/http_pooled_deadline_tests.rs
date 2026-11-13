// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn options(io_timeout_ms: u64) -> HttpRequestOptions {
    HttpRequestOptions {
        connect_timeout_ms: 500,
        io_timeout_ms,
        max_retries: 0,
    }
}

#[test]
fn reused_socket_adopts_both_shorter_and_longer_request_budgets() {
    for (before, after) in [(1000, 25), (25, 1000)] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let stream = connect_fresh(&addr, options(before)).unwrap();
        let local = stream.local_addr().unwrap();
        pool_put(&addr, stream, options(before));
        let expected = connect_fresh(&addr, options(after)).unwrap();
        let reused = pool_take(&addr, options(after)).unwrap().unwrap();
        assert_eq!(reused.local_addr().unwrap(), local);
        // Compare with a fresh socket so the platform's timeout rounding is identical.
        assert_eq!(
            reused.read_timeout().unwrap(),
            expected.read_timeout().unwrap()
        );
        assert_eq!(
            reused.write_timeout().unwrap(),
            expected.write_timeout().unwrap()
        );
    }
}

#[test]
fn unchanged_request_budget_preserves_the_pooled_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = options(75);
    let stream = connect_fresh(&addr, config).unwrap();
    let local = stream.local_addr().unwrap();
    let read = stream.read_timeout().unwrap();
    let write = stream.write_timeout().unwrap();
    pool_put(&addr, stream, config);
    for _ in 0..3 {
        let reused = pool_take(&addr, config).unwrap().unwrap();
        assert_eq!(reused.local_addr().unwrap(), local);
        assert_eq!(reused.read_timeout().unwrap(), read);
        assert_eq!(reused.write_timeout().unwrap(), write);
        pool_put(&addr, reused, config);
        CLIENT_CONN_POOL.with(|pool| {
            assert_eq!(pool.borrow()[&addr].last().unwrap().io_timeout_ms, 75);
        });
    }
    drop(pool_take(&addr, config).unwrap());
}

#[test]
fn invalid_reconfiguration_discards_the_socket_without_sending_a_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let stream = connect_fresh(&addr, options(1000)).unwrap();
    pool_put(&addr, stream, options(1000));
    let error = pool_take(&addr, options(0)).unwrap_err();
    assert!(matches!(error, HttpError::Io(ref error) if error.kind() == ErrorKind::InvalidInput));
    assert!(pool_take(&addr, options(1000)).unwrap().is_none());
    let (mut peer, _) = listener.accept().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    assert_eq!(peer.read(&mut [0u8; 1]).unwrap(), 0);
}
