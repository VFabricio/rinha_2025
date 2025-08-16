#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::{Context, Result};
use config::Config;
use core::ops::DerefMut;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use queue::{Command, QueueClient};
use serde::{Deserialize, Serialize};
use shared::{ConnectionPool, Database};
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[derive(Debug, Deserialize)]
struct GatewayConfig {
    listen_address: SocketAddr,
    queue_url: String,
    queue_connection_pool_size: usize,
    result_directory: String,
}

impl GatewayConfig {
    fn load() -> Result<Self> {
        Config::builder()
            .add_source(config::File::with_name("settings").required(false))
            .add_source(config::Environment::with_prefix("GATEWAY"))
            .build()
            .context("Failed to build configuration.")?
            .try_deserialize()
            .context(
                "Failed to deserialize configuration - check that all required fields are set.",
            )
    }
}

struct Gateway {
    queue_connection_pool: ConnectionPool,
    database: Database,
}

impl Gateway {
    async fn new(config: GatewayConfig) -> Result<Self> {
        let GatewayConfig {
            queue_url,
            queue_connection_pool_size,
            ..
        } = config;

        let queue_connection_pool =
            ConnectionPool::new(queue_url, queue_connection_pool_size).await?;

        let database = Database::new(&config.result_directory)?;
        Ok(Self {
            queue_connection_pool,
            database,
        })
    }

    async fn handle_request(
        &self,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        match (request.method(), request.uri().path()) {
            (&Method::POST, "/payments") => self.handle_payments(request).await,
            (&Method::GET, "/payments-summary") => self.handle_payment_summary(request).await,
            (&Method::POST, "/admin/purge-payments") => self.handle_purge_payments().await,
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"Not Found")))
                .expect("Failed to build 404 response")),
        }
    }

    async fn handle_payments(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let (_parts, body) = req.into_parts();
        let body_bytes = body.collect().await?.to_bytes();

        match self.process_payment(body_bytes).await {
            Ok(_) => Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Full::new(Bytes::new()))
                .expect("Failed to build response")),
            Err(e) => {
                eprintln!("Payment processing error: {}", e);
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from_static(
                        b"{\"error\": \"processing failed\"}",
                    )))
                    .expect("Failed to build error response"))
            }
        }
    }

    async fn process_payment(&self, body_bytes: Bytes) -> Result<()> {
        let connection = self
            .queue_connection_pool
            .try_get_connection()
            .context("No connection to queue are available.")?;
        let mut connection = connection.write().await;
        let connection = connection.deref_mut();

        let mut queue_client = QueueClient::new(connection);

        queue_client
            .send_command(Command::Push(body_bytes.into()))
            .await?;
        connection.set_available();

        Ok(())
    }

    async fn handle_payment_summary(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let uri = req.uri();
        let query = uri.query().unwrap_or("");

        let from_timestamp = parse_timestamp_param(query, "from").unwrap_or(0);
        let to_timestamp = parse_timestamp_param(query, "to").unwrap_or(i64::MAX);

        match self.database.read_all() {
            Ok(results) => {
                let summary = aggregate_results(&results, from_timestamp, to_timestamp);
                let json = serde_json::to_string(&summary).unwrap();

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(json)))
                    .expect("Failed to build response"))
            }
            Err(e) => {
                eprintln!("Failed to read payment results: {}", e);
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from_static(
                        b"{\"error\": \"failed to read results\"}",
                    )))
                    .expect("Failed to build error response"))
            }
        }
    }

    async fn handle_purge_payments(&self) -> Result<Response<Full<Bytes>>, hyper::Error> {
        match self.database.purge_all() {
            Ok(count) => {
                let response_body = format!("{{\"purged\": {}}}", count);
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(response_body)))
                    .expect("Failed to build response"))
            }
            Err(e) => {
                eprintln!("Failed to purge payments: {}", e);
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from_static(
                        b"{\"error\": \"failed to purge payments\"}",
                    )))
                    .expect("Failed to build error response"))
            }
        }
    }
}

fn parse_timestamp_param(query: &str, param: &str) -> Option<i64> {
    for part in query.split('&') {
        if let Some(eq_pos) = part.find('=') {
            let key = &part[..eq_pos];
            let value = &part[eq_pos + 1..];
            if key == param {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                    return Some(dt.timestamp_millis());
                }
            }
        }
    }
    None
}

fn aggregate_results(results: &[shared::PaymentResult], from: i64, to: i64) -> PaymentSummary {
    let mut default_requests = 0;
    let mut default_amount_cents = 0u64;
    let mut fallback_requests = 0;
    let mut fallback_amount_cents = 0u64;

    for result in results {
        if result.timestamp >= from && result.timestamp <= to {
            match result.processor {
                0 => {
                    default_requests += 1;
                    default_amount_cents += result.amount_cents;
                }
                1 => {
                    fallback_requests += 1;
                    fallback_amount_cents += result.amount_cents;
                }
                _ => {}
            }
        }
    }

    PaymentSummary {
        default: ProcessorSummary {
            total_requests: default_requests,
            total_amount: default_amount_cents as f64 / 100.0,
        },
        fallback: ProcessorSummary {
            total_requests: fallback_requests,
            total_amount: fallback_amount_cents as f64 / 100.0,
        },
    }
}

#[derive(Serialize)]
struct PaymentSummary {
    default: ProcessorSummary,
    fallback: ProcessorSummary,
}

#[derive(Serialize)]
struct ProcessorSummary {
    #[serde(rename = "totalRequests")]
    total_requests: u64,
    #[serde(rename = "totalAmount")]
    total_amount: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = GatewayConfig::load().context("Configuration loading failed")?;
    let address = config.listen_address;

    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("Failed to bind to address: {}", address))?;

    println!("Gateway listening on {}", address);

    let gateway = Gateway::new(config)
        .await
        .context("Gateway initialization failed")?;
    let gateway = Box::leak(Box::new(gateway));

    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let io = TokioIo::new(stream);

        tokio::task::spawn({
            async {
                let service = service_fn(|request| gateway.handle_request(request));

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("Error serving connection: {:?}", err);
                }
            }
        });
    }
}
