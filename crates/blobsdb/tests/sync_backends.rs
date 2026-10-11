//! Sync smoke tests against real storage backends, emulated locally.
//!
//! Each test spawns an emulator process, runs a full sync flow against
//! it (upload, verify, idempotent re-sync) and tears the process down.
//!
//! These tests are **gated on the availability of the emulator
//! binaries** in the current environment: if a binary is not on $PATH,
//! the corresponding test prints a notice and passes trivially. No real
//! credentials are needed — every emulator runs with its well-known
//! default credentials.
//!
//! Required binaries:
//!   - S3:    `minio` and `mc`   (https://min.io/)
//!   - Azure: `azurite`          (npm install -g azurite)
//!   - GCS:   `fake-gcs-server`  (https://github.com/fsouza/fake-gcs-server)

use std::{
    collections::HashSet,
    io,
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use blobsdb::{BlobsDB, Chunk, GetObjectStoreInput, ObjPath, get_object_store, get_remote_head};
use object_store::{ObjectStore, ObjectStoreExt};
use tempdir::TempDir;

// ---------------------------------------------------------------------------
// process helpers
// ---------------------------------------------------------------------------

fn binary_available(name: &str) -> bool {
    Command::new(name)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Grab a free port by binding to port 0 and dropping the listener.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Wait until the emulator accepts TCP connections, or give up.
fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Kills the child process on drop.
struct Emulator(Child);

impl Drop for Emulator {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ---------------------------------------------------------------------------
// shared test flow
// ---------------------------------------------------------------------------

/// Full sync flow: multi-chunk upload, remote verification, and an
/// idempotent re-sync. Works against any `ObjectStore` backend.
async fn sync_smoke_flow(remote_name: &str, remote: Arc<dyn ObjectStore>) {
    let tmp = TempDir::new("blobsdb_backend_test").unwrap();
    let blobs_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs_dir).unwrap();
    let store = get_object_store(GetObjectStoreInput::Local).unwrap();
    let db = BlobsDB::new(blobs_dir, tmp.path().join("sync_map"), store).unwrap();

    // deterministic pseudo-random content so chunks have distinct hashes
    let mut state = 0x9E3779B9u32;
    let content: Vec<u8> = (0..1_500_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            (state & 0xFF) as u8
        })
        .collect();

    let file = tmp.path().join("data.bin");
    std::fs::write(&file, &content).unwrap();
    let chunks: HashSet<Chunk> = db.put(&file).await.unwrap().into_iter().collect();
    assert!(chunks.len() > 1, "expected multiple chunks");

    let head = [11u8; 32];
    let mut err_buf = io::stderr();

    // first sync: uploads everything
    db.sync(
        remote_name.to_string(),
        remote.clone(),
        chunks.clone(),
        head,
        b"redb".to_vec(),
        b"history".to_vec(),
        &mut err_buf,
    )
    .await
    .unwrap();

    assert_eq!(get_remote_head(&remote).await.unwrap(), Some(head));

    for c in &chunks {
        let p =
            ObjPath::from_url_path(format!("blobs/{}", blake3::Hash::from_bytes(c.hash))).unwrap();
        remote.head(&p).await.unwrap();
    }

    // second sync: must succeed as a no-op (idempotent re-run)
    db.sync(
        remote_name.to_string(),
        remote.clone(),
        chunks,
        head,
        b"redb".to_vec(),
        b"history".to_vec(),
        &mut err_buf,
    )
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// S3 via MinIO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_against_minio_s3() {
    if !binary_available("minio") || !binary_available("mc") {
        eprintln!("skipping sync_against_minio_s3: `minio` and/or `mc` binaries not found");
        return;
    }

    let tmp = TempDir::new("minio_data").unwrap();
    let port = free_port();
    let endpoint = format!("http://127.0.0.1:{port}");
    let child = Command::new("minio")
        .args(["server", tmp.path().to_str().unwrap()])
        .args(["--address", &format!("127.0.0.1:{port}")])
        .env("MINIO_ROOT_USER", "minioadmin")
        .env("MINIO_ROOT_PASSWORD", "minioadmin")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _emu = Emulator(child);

    assert!(
        wait_for_port(port, Duration::from_secs(15)),
        "minio did not start in time"
    );

    // create the bucket via the MinIO client
    let alias = Command::new("mc")
        .args([
            "alias",
            "set",
            "vertest",
            &endpoint,
            "minioadmin",
            "minioadmin",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(alias.success(), "mc alias set failed");

    let bucket = "ver-sync-test";
    let mb = Command::new("mc")
        .args(["mb", &format!("vertest/{bucket}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(mb.success(), "mc mb failed");

    let store = get_object_store(GetObjectStoreInput::S3 {
        access_key_id: Some("minioadmin".into()),
        secret_access_key: Some("minioadmin".into()),
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint),
        bucket: bucket.into(),
    })
    .unwrap();

    sync_smoke_flow("minio", store).await;
}

// ---------------------------------------------------------------------------
// Azure via Azurite
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_against_azurite_azure() {
    if !binary_available("azurite") || !binary_available("az") {
        eprintln!("skipping sync_against_azurite_azure: `azurite` and/or `az` binary not found");
        return;
    }

    let tmp = TempDir::new("azurite_data").unwrap();
    let port = free_port();
    // --oauth basic makes Azurite accept any bearer token, which lets
    // us create the container with a plain unsigned HTTP request.
    let child = Command::new("azurite")
        .args(["--location", tmp.path().to_str().unwrap()])
        .args(["--blobPort", &port.to_string()])
        .args(["--skipApiVersionCheck"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _emu = Emulator(child);

    assert!(
        wait_for_port(port, Duration::from_secs(15)),
        "azurite did not start in time"
    );

    // Azurite's well-known account and key (documented by Microsoft)
    let account = "devstoreaccount1";
    let key =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    let container = "ver-sync-test";

    let create_container = Command::new("az")
        .args(["storage", "container", "create", "--name", container])
        .env(
            "AZURE_STORAGE_CONNECTION_STRING",
            format!("DefaultEndpointsProtocol=http;AccountName={};AccountKey={};BlobEndpoint=http://127.0.0.1:{}/{};", account, key, port, account),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(create_container.success(), "container creation failed");

    let store = get_object_store(GetObjectStoreInput::Azure {
        account_name: Some(account.into()),
        account_key: Some(key.into()),
        endpoint: Some(format!("http://127.0.0.1:{port}/{account}")),
        container_name: container.into(),
    })
    .unwrap();

    sync_smoke_flow("azurite", store).await;
}

// ---------------------------------------------------------------------------
// GCS via fake-gcs-server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_against_fake_gcs() {
    if !binary_available("fake-gcs-server") {
        eprintln!("skipping sync_against_fake_gcs: `fake-gcs-server` binary not found");
        return;
    }

    let port = free_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let child = Command::new("fake-gcs-server")
        .args(["-scheme", "http"])
        .args(["-port", &port.to_string()])
        .args(["-backend", "memory"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _emu = Emulator(child);

    assert!(
        wait_for_port(port, Duration::from_secs(15)),
        "fake-gcs-server did not start in time"
    );

    let bucket = "ver-sync-test";

    // create the bucket through fake-gcs-server's unauthenticated API
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base_url}/storage/v1/b?project=ver-test"))
        .header("content-type", "application/json")
        .body(serde_json::json!({"name": bucket}).to_string())
        .send()
        .await
        .unwrap();
    assert!(
        res.status().is_success() || res.status() == 409,
        "bucket creation failed: {}",
        res.status()
    );

    // The object_store GCS client reads "gcs_base_url" and
    // "disable_oauth" from the service-account JSON itself, which lets
    // us point it at the emulator without changing GetObjectStoreInput.
    let creds = TempDir::new("gcs_creds").unwrap();
    let creds_path = creds.path().join("emulator.json");
    std::fs::write(
        &creds_path,
        serde_json::json!({
            "type": "service_account",
            "project_id": "ver-test",
            "private_key": "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n",
            "client_email": "test@ver-test.iam.gserviceaccount.com",
            "disable_oauth": true,
            "gcs_base_url": base_url,
        })
        .to_string(),
    )
    .unwrap();

    let store = get_object_store(GetObjectStoreInput::Gcs {
        service_account_credentials_path: creds_path.to_str().unwrap().into(),
        bucket: bucket.into(),
    })
    .unwrap();

    sync_smoke_flow("fake-gcs", store).await;
}
