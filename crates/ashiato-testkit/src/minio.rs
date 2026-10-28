use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// The official minio/minio images are no longer published, so the Bitnami build of a MinIO
// release is used. The tag is the MinIO release, so the server does not change between runs.
pub const MINIO_IMAGE: &str = "bitnamilegacy/minio:2025.7.23";
pub const ACCESS_KEY: &str = "ashiato-test";
pub const SECRET_KEY: &str = "ashiato-test-secret";
pub const REGION: &str = "us-east-1";
pub const CONTAINER_LABEL: &str = "ashiato-test=minio";

const MINIO_PORT: &str = "9000/tcp";
const ALIAS: &str = "local";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const STARTUP_POLL: Duration = Duration::from_millis(200);

static STARTED: AtomicU32 = AtomicU32::new(0);

// Each test starts its own MinIO container and removes it on drop, including when the test
// panics. The container is started with --rm and without volumes, so stopping it leaves
// nothing behind; containers of a killed test run can be removed by CONTAINER_LABEL.
pub struct Minio {
    container: String,
    endpoint: String,
}

impl Minio {
    pub fn start() -> Self {
        let container = format!(
            "ashiato-test-minio-{}-{}",
            std::process::id(),
            STARTED.fetch_add(1, Ordering::Relaxed)
        );
        run(Command::new("docker").args([
            "run",
            "--detach",
            "--rm",
            "--name",
            &container,
            "--label",
            CONTAINER_LABEL,
            "--publish",
            "127.0.0.1::9000",
            "--env",
            &format!("MINIO_ROOT_USER={ACCESS_KEY}"),
            "--env",
            &format!("MINIO_ROOT_PASSWORD={SECRET_KEY}"),
            "--entrypoint",
            "minio",
            MINIO_IMAGE,
            "server",
            "/tmp/data",
        ]));
        let mut minio = Self {
            endpoint: String::new(),
            container,
        };
        let port = run(Command::new("docker").args(["port", &minio.container, MINIO_PORT]));
        let address = String::from_utf8(port.stdout)
            .unwrap()
            .lines()
            .next()
            .expect("docker port prints the published address")
            .trim()
            .to_owned();
        minio.endpoint = format!("http://{address}");
        minio.wait_until_live(&address);
        minio.mc(&[
            "alias",
            "set",
            ALIAS,
            "http://127.0.0.1:9000",
            ACCESS_KEY,
            SECRET_KEY,
        ]);
        minio
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn create_bucket(&self, bucket: &str, versioned: bool) {
        self.mc(&["mb", &self.target(bucket, "")]);
        if versioned {
            self.mc(&["version", "enable", &self.target(bucket, "")]);
        }
    }

    // Copies the files under dir to the bucket with their relative paths as the keys. The
    // staged copy is left in the container, which is removed with everything in it on drop.
    pub fn upload_dir(&self, dir: &Path, bucket: &str) {
        let staged = format!("/tmp/upload-{}", STARTED.fetch_add(1, Ordering::Relaxed));
        self.copy_into_container(dir, &staged);
        self.mc(&[
            "cp",
            "--quiet",
            "--recursive",
            &format!("{staged}/"),
            &self.target(bucket, ""),
        ]);
    }

    pub fn put_file(&self, file: &Path, bucket: &str, key: &str) {
        let staged = format!("/tmp/object-{}", STARTED.fetch_add(1, Ordering::Relaxed));
        self.copy_into_container(file, &staged);
        self.mc(&["cp", "--quiet", &staged, &self.target(bucket, key)]);
    }

    pub fn remove(&self, bucket: &str, key: &str) {
        self.mc(&["rm", "--quiet", &self.target(bucket, key)]);
    }

    pub fn mc(&self, args: &[&str]) -> Output {
        let mut command = vec!["mc"];
        command.extend_from_slice(args);
        self.exec(&command)
    }

    fn exec(&self, args: &[&str]) -> Output {
        run(Command::new("docker")
            .args(["exec", &self.container])
            .args(args))
    }

    fn copy_into_container(&self, source: &Path, destination: &str) {
        run(Command::new("docker")
            .arg("cp")
            .arg(source)
            .arg(format!("{}:{destination}", self.container)));
    }

    fn target(&self, bucket: &str, key: &str) -> String {
        format!("{ALIAS}/{bucket}/{key}")
    }

    fn wait_until_live(&self, address: &str) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        while !is_live(address) {
            assert!(
                Instant::now() < deadline,
                "MinIO in {} did not become live",
                self.container
            );
            thread::sleep(STARTUP_POLL);
        }
    }
}

impl Drop for Minio {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.container])
            .output();
    }
}

// https://min.io/docs/minio/linux/operations/monitoring/healthcheck-probe.html
fn is_live(address: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(address) else {
        return false;
    };
    let request =
        format!("GET /minio/health/live HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    let mut response = String::new();
    stream.write_all(request.as_bytes()).is_ok()
        && stream.read_to_string(&mut response).is_ok()
        && response.starts_with("HTTP/1.1 200")
}

fn run(command: &mut Command) -> Output {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("failed to run {command:?}; is Docker installed? {error}"));
    assert!(
        output.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
