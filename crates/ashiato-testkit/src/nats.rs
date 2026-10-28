use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub const NATS_IMAGE: &str = "nats:2.10.29";
pub const CONTAINER_LABEL: &str = "ashiato-test=nats";

// Without sync_interval: always, a PubAck can come before the message is on disk, and a
// restart of the server loses messages that the workers will not send again (7.1 節).
const CONFIG: &str = "\
port: 4222
jetstream {
  store_dir: /data/jetstream
  sync_interval: always
}
";
const CONFIG_IN_CONTAINER: &str = "/etc/ashiato/nats.conf";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const STARTUP_POLL: Duration = Duration::from_millis(200);
const STOP_TIMEOUT_SECONDS: &str = "1";

static STARTED: AtomicU32 = AtomicU32::new(0);

// Each test starts its own NATS server and removes it on drop, including when the test panics.
// The store is in the container rather than a volume, so a restart keeps the streams and
// removing the container leaves nothing behind; containers of a killed test run can be removed
// by CONTAINER_LABEL. The host port is chosen here rather than by Docker, because Docker can
// publish a restarted container on another port.
pub struct Nats {
    container: String,
    config: PathBuf,
    address: String,
    starts: AtomicUsize,
}

impl Nats {
    pub fn start() -> Self {
        let number = STARTED.fetch_add(1, Ordering::Relaxed);
        let container = format!("ashiato-test-nats-{}-{number}", std::process::id());
        let config = std::env::temp_dir().join(format!("{container}.conf"));
        std::fs::write(&config, CONFIG)
            .unwrap_or_else(|error| panic!("failed to write {config:?}: {error}"));
        let address = format!("127.0.0.1:{}", free_port());
        let nats = Self {
            container,
            config,
            address,
            starts: AtomicUsize::new(0),
        };
        run(Command::new("docker").args([
            "run",
            "--detach",
            "--rm",
            "--name",
            &nats.container,
            "--label",
            CONTAINER_LABEL,
            "--publish",
            &format!("{}:4222", nats.address),
            "--volume",
            &format!("{}:{CONFIG_IN_CONTAINER}:ro", nats.config.display()),
            NATS_IMAGE,
            "--config",
            CONFIG_IN_CONTAINER,
        ]));
        nats.wait_until_ready();
        nats
    }

    pub fn url(&self) -> String {
        format!("nats://{}", self.address)
    }

    // Stops the server and starts it again with the same store, as a crash and a restart of
    // the server would (10 節).
    pub fn restart(&self) {
        run(Command::new("docker").args([
            "restart",
            "--time",
            STOP_TIMEOUT_SECONDS,
            &self.container,
        ]));
        self.wait_until_ready();
    }

    // The server logs that it is ready once JetStream has recovered its streams, which is later
    // than it accepts connections. The log of a restarted container keeps the earlier starts.
    fn wait_until_ready(&self) {
        let starts = self.starts.fetch_add(1, Ordering::Relaxed) + 1;
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        while self.ready_count() < starts {
            assert!(
                Instant::now() < deadline,
                "NATS server in {} did not start",
                self.container
            );
            thread::sleep(STARTUP_POLL);
        }
    }
}

impl Drop for Nats {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.container])
            .output();
        let _ = std::fs::remove_file(&self.config);
    }
}

impl Nats {
    fn ready_count(&self) -> usize {
        let Ok(output) = Command::new("docker")
            .args(["logs", &self.container])
            .output()
        else {
            return 0;
        };
        String::from_utf8_lossy(&output.stderr)
            .lines()
            .chain(String::from_utf8_lossy(&output.stdout).lines())
            .filter(|line| line.contains("Server is ready"))
            .count()
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("a free port is available on 127.0.0.1")
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
