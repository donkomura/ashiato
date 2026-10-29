use std::collections::HashMap;
use std::sync::Arc;

use ashiato_pipeline::{
    AnalyzerMetrics, CommittedPositions, ControlClient, ControlError, Manifest, ManifestEntry,
    ManifestPage, ManifestRequest, PartitionId, Position, ResumeRequest, ResumeResponse, RunPlan,
    TransportError, WorkerPlan, manifest_page, resume_response,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;

use crate::broker::{EventStream, NatsError, RetryPolicy, checked_run_id, retrying};

const PLAN: &str = "plan";
const MANIFEST: &str = "manifest";
const RESUME: &str = "resume";

// Leaves room in a reply of the manifest for the envelope of Reply and the protocol line.
const REPLY_ALLOWANCE_BYTES: usize = 1024;

pub fn control_subject(run_id: &str, request: &str) -> String {
    format!("ashiato.{run_id}.control.{request}")
}

// A refusal carries its reason, so that a worker started with a wrong id stops with the reason
// instead of asking again until it gives up.
#[derive(Serialize, Deserialize)]
enum Reply<T> {
    Answer(T),
    Refused(String),
}

// The answers of the analyzer to the requests of the ingest workers (7.4 節).
pub struct ControlService {
    plans: Vec<WorkerPlan>,
    manifest: Manifest,
    committed: CommittedPositions,
    metrics: Arc<AnalyzerMetrics>,
}

impl ControlService {
    pub fn new(
        plan: &RunPlan,
        manifest: Manifest,
        committed: CommittedPositions,
    ) -> Result<Self, ControlError> {
        let partitions: Vec<PartitionId> = manifest
            .partitions
            .iter()
            .map(|partition| partition.partition.clone())
            .collect();
        let plans = (0..plan.placement.ingest_workers.get())
            .map(|worker_id| WorkerPlan::new(plan, &partitions, worker_id))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            plans,
            manifest,
            committed,
            metrics: Arc::default(),
        })
    }

    pub fn with_metrics(self, metrics: Arc<AnalyzerMetrics>) -> Self {
        Self { metrics, ..self }
    }

    fn answer(&self, request: &str, body: &[u8], limit: usize) -> Vec<u8> {
        let reply = match request {
            PLAN => self.plan(body).map(|plan| {
                self.metrics.first_plan_response.mark();
                encode(&plan)
            }),
            MANIFEST => self.manifest_page(body, limit).map(|page| encode(&page)),
            RESUME => self.resume(body).map(|resume| encode(&resume)),
            _ => Err(format!("unknown control request {request}")),
        };
        match reply {
            Ok(Ok(answer)) => answer,
            Ok(Err(reason)) | Err(reason) => encode_reply::<()>(Reply::Refused(reason)),
        }
    }

    fn plan(&self, body: &[u8]) -> Result<WorkerPlan, String> {
        let worker_id: u32 = decode_request(body)?;
        let workers = u32::try_from(self.plans.len()).unwrap_or(u32::MAX);
        usize::try_from(worker_id)
            .ok()
            .and_then(|slot| self.plans.get(slot))
            .cloned()
            .ok_or_else(|| ControlError::UnknownWorker { worker_id, workers }.to_string())
    }

    fn manifest_page(&self, body: &[u8], limit: usize) -> Result<ManifestPage, String> {
        let request: ManifestRequest = decode_request(body)?;
        let partition = self
            .manifest
            .partitions
            .iter()
            .find(|partition| partition.partition == request.partition)
            .ok_or_else(|| ControlError::UnknownPartition(request.partition.clone()).to_string())?;
        manifest_page(&partition.files, request.from_file_seq, limit)
            .map_err(|error| error.to_string())
    }

    fn resume(&self, body: &[u8]) -> Result<ResumeResponse, String> {
        let request: ResumeRequest = decode_request(body)?;
        let partitions: Vec<PartitionId> = self
            .manifest
            .partitions
            .iter()
            .map(|partition| partition.partition.clone())
            .collect();
        resume_response(&request, &partitions, &self.committed.get())
            .map_err(|error| error.to_string())
    }
}

fn decode_request<T: DeserializeOwned>(body: &[u8]) -> Result<T, String> {
    serde_json::from_slice(body).map_err(|error| format!("malformed control request: {error}"))
}

fn encode<T: Serialize>(answer: &T) -> Result<Vec<u8>, String> {
    Ok(encode_reply(Reply::Answer(answer)))
}

fn encode_reply<T: Serialize>(reply: Reply<T>) -> Vec<u8> {
    serde_json::to_vec(&reply).expect("the replies are plain data that JSON can hold")
}

// Answers until it is dropped. The subscription is made before this returns, so that a worker
// asking right after the start of the run is answered.
pub struct ControlServer {
    task: JoinHandle<()>,
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn serve_control(
    client: async_nats::Client,
    run_id: &str,
    service: ControlService,
) -> Result<ControlServer, NatsError> {
    let run_id = checked_run_id(run_id)?;
    let subject = control_subject(&run_id, "*");
    let mut requests =
        client
            .subscribe(subject.clone())
            .await
            .map_err(|source| NatsError::Subscribe {
                subject,
                source: source.into(),
            })?;
    client
        .flush()
        .await
        .map_err(|source| NatsError::Subscribe {
            subject: control_subject(&run_id, "*"),
            source: source.into(),
        })?;
    let limit = client.max_payload().saturating_sub(REPLY_ALLOWANCE_BYTES);
    let task = tokio::spawn(async move {
        while let Some(request) = requests.next().await {
            let Some(reply) = request.reply else {
                continue;
            };
            let name = request.subject.rsplit('.').next().unwrap_or_default();
            let answer = service.answer(name, &request.payload, limit);
            // A reply that does not reach the worker makes it ask again.
            let _ = client.publish(reply, Bytes::from(answer)).await;
        }
    });
    Ok(ControlServer { task })
}

// The requests are sent again until the analyzer answers, because the workers may start before
// the analyzer or while it restarts (7.4 節).
pub struct NatsControl {
    client: async_nats::Client,
    run_id: String,
    stream: OnceCell<EventStream>,
    retry: RetryPolicy,
}

impl NatsControl {
    pub fn new(client: async_nats::Client, run_id: &str) -> Result<Self, NatsError> {
        Ok(Self {
            client,
            run_id: checked_run_id(run_id)?,
            stream: OnceCell::new(),
            retry: RetryPolicy::default(),
        })
    }

    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry, ..self }
    }

    async fn request<Q: Serialize, A: DeserializeOwned>(
        &self,
        request: &str,
        body: &Q,
    ) -> Result<A, NatsError> {
        let subject = control_subject(&self.run_id, request);
        let payload = Bytes::from(serde_json::to_vec(body).map_err(NatsError::ControlReply)?);
        let reply = retrying(self.retry, || async {
            self.client
                .request(subject.clone(), payload.clone())
                .await
                .map_err(|source| NatsError::Request {
                    subject: subject.clone(),
                    source: source.into(),
                })
        })
        .await?;
        match serde_json::from_slice(&reply.payload).map_err(NatsError::ControlReply)? {
            Reply::Answer(answer) => Ok(answer),
            Reply::Refused(reason) => Err(NatsError::Refused { subject, reason }),
        }
    }
}

#[async_trait]
impl ControlClient for NatsControl {
    async fn plan(&self, worker_id: u32) -> Result<WorkerPlan, TransportError> {
        Ok(self.request(PLAN, &worker_id).await?)
    }

    async fn manifest(
        &self,
        partition: &PartitionId,
        from_file_seq: u64,
    ) -> Result<Vec<ManifestEntry>, TransportError> {
        let mut files = Vec::new();
        let mut next = Some(from_file_seq);
        while let Some(from_file_seq) = next {
            let page: ManifestPage = self
                .request(
                    MANIFEST,
                    &ManifestRequest {
                        partition: partition.clone(),
                        from_file_seq,
                    },
                )
                .await?;
            files.extend(page.files);
            next = page.next_file_seq;
        }
        Ok(files)
    }

    async fn resume_positions(
        &self,
        partitions: &[PartitionId],
    ) -> Result<HashMap<PartitionId, Position>, TransportError> {
        let response: ResumeResponse = self
            .request(
                RESUME,
                &ResumeRequest {
                    partitions: partitions.to_vec(),
                },
            )
            .await?;
        Ok(response
            .positions
            .into_iter()
            .map(|position| (position.partition, position.position))
            .collect())
    }

    // The stream is opened on the first use, since it exists only once the analyzer has
    // answered the plan.
    async fn last_published(
        &self,
        partition: &PartitionId,
    ) -> Result<Option<Position>, TransportError> {
        let stream = self
            .stream
            .get_or_try_init(|| {
                retrying(self.retry, || {
                    EventStream::open(self.client.clone(), &self.run_id)
                })
            })
            .await?;
        Ok(stream.last_published(partition).await?)
    }
}
