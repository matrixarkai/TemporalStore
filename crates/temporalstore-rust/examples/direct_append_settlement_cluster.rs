// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Check append attempt settlement against a real replicated cluster.
//!
//! MODE=direct uses the normal direct door; MODE=gateway uses GATEWAY_ADDR.
//! META and TS_MATRIXOBJECT_* configure the direct door. Use a fresh PREFIX.
//! Both modes use the shared SDK through the TemporalStore object-store adapter.
//! One append is cut before admission, another after a successful response. The
//! actual resolve replies must prove absent and landed, with exact bytes/offsets.
//! Optional CONTROL_DIR barriers let a local controller restart metadata while
//! each append is still unacknowledged. Each barrier is bounded to thirty seconds.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use matrixobject_client::wire::{ATTEMPT_HEADER, WRITE_OFFSET_HEADER};
use matrixobject_client::{Reply, TcpTransport, Transport, TransportError};
use temporalstore_rust::matrixobject_direct::DirectConfig;
use temporalstore_snapshot::object_store::{
    MatrixObjectHttpStore, ObjectStore, DEFAULT_HTTP_BUCKET,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cut {
    BeforeAdmission,
    AfterCommit,
}
impl Cut {
    fn name(self) -> &'static str {
        match self {
            Self::BeforeAdmission => "before-admission",
            Self::AfterCommit => "after-commit",
        }
    }
}

#[derive(Debug)]
struct CutTransport {
    inner: Arc<dyn Transport>,
    target: String,
    arm: Mutex<Option<Cut>>,
    observed: Mutex<Vec<Cut>>,
    forwarded_puts: AtomicUsize,
    control: Option<PathBuf>,
    successful_cut_forwarded: AtomicUsize,
}
impl CutTransport {
    fn barrier(&self, cut: Cut, status: Option<u16>) {
        if let Some(directory) = &self.control {
            std::fs::create_dir_all(directory).expect("create controller directory");
            let name = cut.name();
            let temporary = directory.join(format!("{name}.tmp"));
            let ready = directory.join(format!("{name}.ready.json"));
            let proceed = directory.join(format!("{name}.continue"));
            assert!(
                !ready.exists() && !proceed.exists(),
                "use a fresh control directory"
            );
            std::fs::write(
                &temporary,
                serde_json::to_vec(&serde_json::json!({
                    "phase": name, "status": status, "target": self.target,
                    "forwarded_puts": self.forwarded_puts.load(Ordering::SeqCst),
                }))
                .unwrap(),
            )
            .unwrap();
            std::fs::rename(temporary, ready).unwrap();
            let deadline = Instant::now() + Duration::from_secs(30);
            while !proceed.exists() {
                assert!(
                    Instant::now() < deadline,
                    "controller did not release {name}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
impl Transport for CutTransport {
    fn send(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply, TransportError> {
        let append = method == "PUT"
            && target == self.target
            && headers
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case(WRITE_OFFSET_HEADER))
            && headers
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case(ATTEMPT_HEADER));
        let cut = if append {
            self.arm.lock().unwrap().take()
        } else {
            None
        };
        if cut == Some(Cut::BeforeAdmission) {
            self.barrier(Cut::BeforeAdmission, None);
            self.observed.lock().unwrap().push(Cut::BeforeAdmission);
            return Err(TransportError(
                "controlled request loss before admission".into(),
            ));
        }
        if append {
            self.forwarded_puts.fetch_add(1, Ordering::SeqCst);
        }
        let reply = match self.inner.send(method, target, headers, body) {
            Ok(reply) => reply,
            Err(error) => {
                if cut == Some(Cut::AfterCommit) {
                    *self.arm.lock().unwrap() = cut;
                }
                return Err(error);
            }
        };
        if cut == Some(Cut::AfterCommit) {
            if !matches!(reply.status, 200 | 201 | 204) {
                *self.arm.lock().unwrap() = cut;
                return Ok(reply);
            }
            self.successful_cut_forwarded
                .store(self.forwarded_puts.load(Ordering::SeqCst), Ordering::SeqCst);
            self.barrier(Cut::AfterCommit, Some(reply.status));
            self.observed.lock().unwrap().push(Cut::AfterCommit);
            return Err(TransportError(
                "controlled reply loss after successful write".into(),
            ));
        }
        Ok(reply)
    }
}

#[tokio::main]
async fn main() {
    let mode = std::env::var("MODE").unwrap_or_else(|_| "direct".into());
    let bucket =
        std::env::var("TS_MATRIXOBJECT_BUCKET").unwrap_or_else(|_| DEFAULT_HTTP_BUCKET.into());
    let prefix = std::env::var("PREFIX").expect("fresh PREFIX");
    assert!(
        !prefix.is_empty()
            && prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
        "PREFIX must be a plain fresh name"
    );
    let key = format!("{prefix}/settlement");
    let inner: Arc<dyn Transport> = match mode.as_str() {
        "direct" => {
            let meta = std::env::var("META").expect("META endpoints");
            DirectConfig::from_env(&meta)
                .unwrap()
                .connect_door()
                .unwrap()
        }
        "gateway" => Arc::new(TcpTransport::new(
            std::env::var("GATEWAY_ADDR").expect("GATEWAY_ADDR"),
        )),
        _ => panic!("MODE must be direct or gateway"),
    };
    let transport = Arc::new(CutTransport {
        inner,
        target: format!("/{bucket}/{key}"),
        arm: Mutex::new(None),
        observed: Mutex::new(Vec::new()),
        forwarded_puts: AtomicUsize::new(0),
        control: std::env::var_os("CONTROL_DIR").map(PathBuf::from),
        successful_cut_forwarded: AtomicUsize::new(0),
    });
    let store = MatrixObjectHttpStore::in_process(transport.clone(), &bucket);
    let seed = b"seed\n";
    let initial = store
        .append_blob(&key, Bytes::from_static(seed))
        .await
        .expect("seed append");
    assert_eq!(
        (initial.start_offset, initial.end_offset),
        (0, seed.len() as u64)
    );
    let mut expected = seed.to_vec();
    for (cut, bytes) in [
        (Cut::BeforeAdmission, b"before\n".as_slice()),
        (Cut::AfterCommit, b"after\n".as_slice()),
    ] {
        let before = store.append_resolutions();
        let puts_before = transport.forwarded_puts.load(Ordering::SeqCst);
        *transport.arm.lock().unwrap() = Some(cut);
        let receipt = store
            .append_blob(&key, Bytes::copy_from_slice(bytes))
            .await
            .expect("SDK must settle the controlled append");
        assert_eq!(receipt.start_offset, expected.len() as u64);
        expected.extend_from_slice(bytes);
        assert_eq!(receipt.end_offset, expected.len() as u64);
        assert_eq!(receipt.bytes_written, bytes.len() as u64);
        assert_eq!(receipt.object_length, expected.len() as u64);
        let after = store.append_resolutions();
        let landed = after.landed - before.landed;
        let absent = after.absent - before.absent;
        let ambiguous = after.ambiguous - before.ambiguous;
        assert_eq!(
            ambiguous, 0,
            "every completed append must have a decisive outcome"
        );
        match cut {
            Cut::BeforeAdmission => {
                assert!(absent >= 1, "the unadmitted attempt must be fenced out")
            }
            Cut::AfterCommit => {
                assert!(
                    landed >= 1,
                    "the successful write with its lost reply must be identified"
                );
                assert_eq!(transport.forwarded_puts.load(Ordering::SeqCst), transport.successful_cut_forwarded.load(Ordering::SeqCst),
                    "the committed append must not be forwarded again after losing its successful response");
            }
        }
        let forwarded = transport.forwarded_puts.load(Ordering::SeqCst) - puts_before;
        assert!(forwarded >= 1);
        assert_eq!(store.get(&key).await.unwrap().as_ref(), expected.as_slice());
        println!("SETTLED mode={mode} cut={} landed={landed} absent={absent} ambiguous={ambiguous} forwarded={forwarded} offset={} end={}", cut.name(), receipt.start_offset, receipt.end_offset);
    }
    assert_eq!(
        *transport.observed.lock().unwrap(),
        vec![Cut::BeforeAdmission, Cut::AfterCommit]
    );
    assert!(transport.forwarded_puts.load(Ordering::SeqCst) >= 3);
    assert_eq!(
        store.get(&key).await.unwrap().as_ref(),
        b"seed\nbefore\nafter\n"
    );
    let answers = store.append_resolutions();
    assert!(answers.landed >= 1 && answers.absent >= 1 && answers.ambiguous == 0);
    println!(
        "RESULT mode={mode} cuts=2 forwarded={} landed={} absent={} ambiguous=0 exact_bytes=true",
        transport.forwarded_puts.load(Ordering::SeqCst),
        answers.landed,
        answers.absent
    );
}
