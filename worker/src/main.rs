#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::{Context, Result};
use chrono::DateTime;
use config::Config;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use serde::Deserialize;
use shared::{Database, Payment, PaymentResult, Queue};
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Strategy {
    Greedy,
}

impl FromStr for Strategy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "greedy" => Ok(Strategy::Greedy),
            _ => Err(anyhow::anyhow!("Unknown strategy: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct WorkerConfig {
    valkey_url: String,
    valkey_queue_name: String,
    default_provider_url: String,
    fallback_provider_url: String,
    strategy: Strategy,
    worker_count: usize,
    provider_timeout_ms: u64,
    result_directory: String,
}

impl WorkerConfig {
    fn load() -> Result<Self> {
        Config::builder()
            .add_source(config::File::with_name("settings").required(false))
            .add_source(config::Environment::with_prefix("WORKER"))
            .build()
            .context("Failed to build configuration.")?
            .try_deserialize()
            .context(
                "Failed to deserialize configuration - check that all required fields are set.",
            )
    }
}

struct PaymentWorker {
    http_client: Client<HttpConnector, Full<Bytes>>,
    strategy: Strategy,
    queue: Queue,
    default_provider_url: String,
    fallback_provider_url: String,
    provider_timeout_ms: u64,
    worker_id: usize,
}

struct QueueMonitor {
    queue: Queue,
}

impl QueueMonitor {
    async fn new(valkey_url: &str, queue_name: &str) -> Result<Self> {
        let queue = Queue::new(valkey_url, queue_name).await?;
        Ok(Self { queue })
    }

    async fn run(mut self) -> Result<()> {
        loop {
            match self.queue.get_queue_size().await {
                Ok(queue_size) => {
                    println!("Queue size: {}", queue_size);
                }
                Err(e) => {
                    eprintln!("Failed to get queue size: {}", e);
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

impl PaymentWorker {
    async fn new(config: &WorkerConfig, worker_id: usize) -> Result<Self> {
        let WorkerConfig {
            valkey_url,
            valkey_queue_name,
            default_provider_url,
            fallback_provider_url,
            strategy,
            provider_timeout_ms,
            ..
        } = config;

        let queue = Queue::new(valkey_url, valkey_queue_name).await?;

        let http_client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .build(HttpConnector::new());

        Ok(Self {
            http_client,
            strategy: *strategy,
            queue,
            default_provider_url: default_provider_url.clone(),
            fallback_provider_url: fallback_provider_url.clone(),
            provider_timeout_ms: *provider_timeout_ms,
            worker_id,
        })
    }

    async fn run(mut self, database: std::sync::Arc<Database>) -> Result<()> {
        println!("Worker started with {:?} strategy", self.strategy);

        loop {
            match self.queue.get_payment().await {
                Ok(payment) => {
                    if self.process_payment(&payment, &database).await.is_err() {
                        let _ = self.queue.put_payment(&payment).await;
                    }
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn process_payment(&mut self, payment: &Payment, database: &Database) -> Result<()> {
        let provider_url = match self.strategy {
            Strategy::Greedy => &self.default_provider_url,
        };

        let requested_at = DateTime::to_rfc3339(
            &DateTime::from_timestamp_millis(payment.timestamp as i64)
                .context("Invalid timestamp.")?,
        );

        let request_body = serde_json::json!({
            "correlationId": payment.correlation_id,
            "amount": payment.amount_cents as f64 / 100.0,
            "requestedAt": requested_at,
        });

        let req = Request::post(format!("{}/payments", provider_url))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(request_body.to_string())))
            .context("Failed to build payment request")?;

        let response = tokio::time::timeout(
            Duration::from_millis(self.provider_timeout_ms),
            self.http_client.request(req),
        )
        .await
        .context("Provider request timed out")?
        .context("Provider request failed")?;

        if response.status() != StatusCode::OK {
            return Err(anyhow::anyhow!(
                "Payment failed: status={}",
                response.status()
            ));
        }

        let processor = match self.strategy {
            Strategy::Greedy => 0u8,
        };

        let payment_result = PaymentResult {
            timestamp: payment.timestamp,
            amount_cents: payment.amount_cents,
            processor,
        };

        database.log_payment_result(self.worker_id, &payment_result)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = WorkerConfig::load().context("Configuration loading failed")?;
    let worker_count = config.worker_count;

    println!("Starting {} payment workers", worker_count);

    let mut database =
        Database::new(&config.result_directory).context("Failed to initialize database")?;
    database
        .create_files(worker_count)
        .context("Failed to create database files")?;
    let database = std::sync::Arc::new(database);

    let mut tasks = Vec::new();

    for i in 0..worker_count {
        let config = config.clone();
        let database = database.clone();
        tasks.push(tokio::spawn(async move {
            let worker = PaymentWorker::new(&config, i).await.unwrap();
            println!("Worker {} started", i);
            if let Err(e) = worker.run(database).await {
                eprintln!("Worker {} failed: {}", i, e);
            }
        }));
    }

    tasks.push(tokio::spawn(async move {
        let config = config.clone();
        let monitor = QueueMonitor::new(&config.valkey_url, &config.valkey_queue_name)
            .await
            .unwrap();
        if let Err(e) = monitor.run().await {
            eprintln!("Error running queue monitor: {}.", e);
        }
    }));

    let results = futures::future::join_all(tasks).await;

    for result in results {
        result.context("Worker task failed")?;
    }

    Ok(())
}
