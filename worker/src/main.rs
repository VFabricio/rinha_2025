#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::{Context, Result};
use chrono::DateTime;
use config::Config;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use serde::Deserialize;
use shared::{Database, Payment, PaymentResult, Queue};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Strategy {
    Greedy,
    Quick,
}

impl FromStr for Strategy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "greedy" => Ok(Strategy::Greedy),
            "quick" => Ok(Strategy::Quick),
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
    health_check_interval_ms: u64,
    failure_sleep_ms: u64,
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

#[derive(Debug)]
struct ProcessorHealth {
    is_failing: AtomicBool,
    latency_ms: AtomicU64,
}

impl ProcessorHealth {
    fn new() -> Self {
        Self {
            is_failing: AtomicBool::new(false),
            latency_ms: AtomicU64::new(0),
        }
    }

    fn mark_failing(&self) {
        self.is_failing.store(true, Ordering::Release);
    }

    fn mark_healthy(&self, latency_ms: u64) {
        self.is_failing.store(false, Ordering::Release);
        self.latency_ms.store(latency_ms, Ordering::Release);
    }

    fn update_latency_from_timeout(&self, timeout_latency_ms: u64) {
        let current = self.latency_ms.load(Ordering::Acquire);
        if timeout_latency_ms > current {
            self.latency_ms.store(timeout_latency_ms, Ordering::Release);
        }
    }

    fn is_failing(&self) -> bool {
        self.is_failing.load(Ordering::Acquire)
    }

    fn latency_ms(&self) -> u64 {
        self.latency_ms.load(Ordering::Acquire)
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
    default_health: Arc<ProcessorHealth>,
    fallback_health: Arc<ProcessorHealth>,
    failure_sleep_ms: u64,
}

struct HealthChecker {
    http_client: Client<HttpConnector, Full<Bytes>>,
    default_provider_url: String,
    fallback_provider_url: String,
    default_health: Arc<ProcessorHealth>,
    fallback_health: Arc<ProcessorHealth>,
    check_interval_ms: u64,
    provider_timeout_ms: u64,
}

impl HealthChecker {
    fn new(
        default_provider_url: String,
        fallback_provider_url: String,
        default_health: Arc<ProcessorHealth>,
        fallback_health: Arc<ProcessorHealth>,
        check_interval_ms: u64,
        provider_timeout_ms: u64,
    ) -> Self {
        let http_client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .build(HttpConnector::new());

        Self {
            http_client,
            default_provider_url,
            fallback_provider_url,
            default_health,
            fallback_health,
            check_interval_ms,
            provider_timeout_ms,
        }
    }

    async fn run(self) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(self.check_interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            
            let default_check = self.check_processor_health(&self.default_provider_url, &self.default_health);
            let fallback_check = self.check_processor_health(&self.fallback_provider_url, &self.fallback_health);
            
            let _ = tokio::join!(default_check, fallback_check);
        }
    }

    async fn check_processor_health(&self, provider_url: &str, health: &ProcessorHealth) {
        let req = Request::get(format!("{}/payments/service-health", provider_url))
            .body(Full::new(Bytes::new()))
            .expect("Failed to build health check request");

        let start_time = Instant::now();
        let response_result = tokio::time::timeout(
            Duration::from_millis(self.provider_timeout_ms),
            self.http_client.request(req),
        ).await;

        match response_result {
            Ok(Ok(response)) => {
                let latency_ms = start_time.elapsed().as_millis() as u64;
                
                if response.status().is_success() {
                    if let Ok(body) = response.into_body().collect().await {
                        if let Ok(health_response) = serde_json::from_slice::<HealthResponse>(&body.to_bytes()) {
                            let actual_latency = std::cmp::max(latency_ms, health_response.min_response_time);
                            
                            if health_response.failing {
                                health.mark_failing();
                            } else {
                                health.mark_healthy(actual_latency);
                            }
                            return;
                        }
                    }
                }
                health.mark_failing();
            }
            Ok(Err(_)) => {
                health.mark_failing();
            }
            Err(_) => {
                let timeout_latency = self.provider_timeout_ms;
                health.update_latency_from_timeout(timeout_latency);
                health.mark_failing();
            }
        }
    }
}

#[derive(Deserialize)]
struct HealthResponse {
    failing: bool,
    #[serde(rename = "minResponseTime")]
    min_response_time: u64,
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
    fn new(
        config: &WorkerConfig,
        worker_id: usize,
        queue: Queue,
        default_health: Arc<ProcessorHealth>,
        fallback_health: Arc<ProcessorHealth>,
    ) -> Self {
        let http_client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .build(HttpConnector::new());

        Self {
            http_client,
            strategy: config.strategy,
            queue,
            default_provider_url: config.default_provider_url.clone(),
            fallback_provider_url: config.fallback_provider_url.clone(),
            provider_timeout_ms: config.provider_timeout_ms,
            worker_id,
            default_health,
            fallback_health,
            failure_sleep_ms: config.failure_sleep_ms,
        }
    }

    async fn run(mut self, database: std::sync::Arc<Database>) -> Result<()> {
        println!("Worker started with {:?} strategy", self.strategy);

        loop {
            if matches!(self.strategy, Strategy::Quick) {
                if self.default_health.is_failing() && self.fallback_health.is_failing() {
                    tokio::time::sleep(Duration::from_millis(self.failure_sleep_ms)).await;
                    continue;
                }
            }

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
        let (provider_url, processor_id) = match self.strategy {
            Strategy::Greedy => (&self.default_provider_url, 0u8),
            Strategy::Quick => self.select_processor_quick(),
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

        let _start_time = Instant::now();
        let response_result = tokio::time::timeout(
            Duration::from_millis(self.provider_timeout_ms),
            self.http_client.request(req),
        )
        .await;

        match response_result {
            Ok(Ok(response)) => {
                if !response.status().is_success() {
                    if processor_id == 0 {
                        self.default_health.mark_failing();
                    } else {
                        self.fallback_health.mark_failing();
                    }
                    return Err(anyhow::anyhow!(
                        "Payment failed: status={}",
                        response.status()
                    ));
                }
            }
            Ok(Err(_)) => {
                if processor_id == 0 {
                    self.default_health.mark_failing();
                } else {
                    self.fallback_health.mark_failing();
                }
                return Err(anyhow::anyhow!("Provider request failed"));
            }
            Err(_) => {
                let timeout_latency = self.provider_timeout_ms;
                if processor_id == 0 {
                    self.default_health.update_latency_from_timeout(timeout_latency);
                    self.default_health.mark_failing();
                } else {
                    self.fallback_health.update_latency_from_timeout(timeout_latency);
                    self.fallback_health.mark_failing();
                }
                return Err(anyhow::anyhow!("Provider request timed out"));
            }
        }

        let payment_result = PaymentResult {
            timestamp: payment.timestamp,
            amount_cents: payment.amount_cents,
            processor: processor_id,
        };

        database.log_payment_result(self.worker_id, &payment_result)?;

        Ok(())
    }

    fn select_processor_quick(&self) -> (&String, u8) {
        let default_failing = self.default_health.is_failing();
        let fallback_failing = self.fallback_health.is_failing();

        match (default_failing, fallback_failing) {
            (true, true) => (&self.default_provider_url, 0),
            (false, true) => (&self.default_provider_url, 0),
            (true, false) => (&self.fallback_provider_url, 1),
            (false, false) => {
                let default_latency = self.default_health.latency_ms();
                let fallback_latency = self.fallback_health.latency_ms();

                if default_latency == 0 && fallback_latency == 0 {
                    return (&self.default_provider_url, 0);
                }

                let default_weight = if default_latency == 0 { 1000 } else { 1000 / default_latency };
                let fallback_weight = if fallback_latency == 0 { 1000 } else { 1000 / fallback_latency };
                
                let total_weight = default_weight + fallback_weight;
                let random_val = fastrand::u64(0..total_weight);

                if random_val < default_weight {
                    (&self.default_provider_url, 0)
                } else {
                    (&self.fallback_provider_url, 1)
                }
            }
        }
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

    let default_health = Arc::new(ProcessorHealth::new());
    let fallback_health = Arc::new(ProcessorHealth::new());

    let mut tasks = Vec::new();

    if matches!(config.strategy, Strategy::Quick) {
        let health_checker = HealthChecker::new(
            config.default_provider_url.clone(),
            config.fallback_provider_url.clone(),
            default_health.clone(),
            fallback_health.clone(),
            config.health_check_interval_ms,
            config.provider_timeout_ms,
        );
        
        tasks.push(tokio::spawn(async move {
            if let Err(e) = health_checker.run().await {
                eprintln!("Health checker failed: {}", e);
            }
        }));
    }

    for i in 0..worker_count {
        let config = config.clone();
        let database = database.clone();
        let default_health = default_health.clone();
        let fallback_health = fallback_health.clone();
        
        tasks.push(tokio::spawn(async move {
            let queue = Queue::new(&config.valkey_url, &config.valkey_queue_name)
                .await
                .expect("Failed to create queue");
            
            let worker = PaymentWorker::new(&config, i, queue, default_health, fallback_health);
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
