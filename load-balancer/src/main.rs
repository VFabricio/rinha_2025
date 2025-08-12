use anyhow::{Context, Result};
use config::Config;
use http::uri::Authority;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

fn deserialize_uris<'de, D>(deserializer: D) -> Result<Vec<Authority>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let items: Vec<String> = Vec::deserialize(deserializer)?;
    let mut result = Vec::with_capacity(items.len());
    for item in items {
        result.push(
            item.parse()
                .map_err(|_| serde::de::Error::custom(format!("{item} is not a valid URI.")))?,
        )
    }
    Ok(result)
}

#[derive(Debug, Deserialize)]
struct AppConfig {
    listen_address: SocketAddr,
    #[serde(deserialize_with = "deserialize_uris")]
    upstream_addresses: Vec<Authority>,
    upstream_timeout_ms: u64,
    circuit_breaker_failure_threshold: u32,
    circuit_breaker_recovery_timeout_ms: u64,
}

#[derive(Debug)]
struct CircuitState {
    consecutive_failures: AtomicU32,
    last_failure_time_ms: AtomicU64,
    is_open: AtomicBool,
}

impl CircuitState {
    fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            last_failure_time_ms: AtomicU64::new(0),
            is_open: AtomicBool::new(false),
        }
    }
}

impl AppConfig {
    fn load() -> Result<Self> {
        Config::builder()
            .add_source(config::File::with_name("settings").required(false))
            .add_source(config::Environment::with_prefix("LB"))
            .build()
            .context("Failed to build configuration.")?
            .try_deserialize()
            .context(
                "Failed to deserialize configuration - check that all required fields are set.",
            )
    }
}

struct LoadBalancer {
    http_client: Client<HttpConnector, Incoming>,
    next_upstream_index: AtomicUsize,
    upstream_states: Vec<CircuitState>,
    upstream_addresses: Vec<Authority>,
    upstream_timeout_ms: Duration,
    recovery_timeout_ms: u64,
    failure_threshold: u32,
}

enum ForwardError {
    NoHealthyUpstreams,
    Timeout,
    UpstreamError,
}

impl LoadBalancer {
    fn new(config: AppConfig) -> Result<Self> {
        let connector = HttpConnector::new();
        let http_client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .build(connector);

        let AppConfig {
            upstream_addresses, ..
        } = config;

        let upstream_states = (0..upstream_addresses.len())
            .map(|_| CircuitState::new())
            .collect();

        Ok(LoadBalancer {
            http_client,
            next_upstream_index: AtomicUsize::new(0),
            upstream_states,
            upstream_addresses,
            upstream_timeout_ms: Duration::from_millis(config.upstream_timeout_ms),
            recovery_timeout_ms: config.circuit_breaker_recovery_timeout_ms,
            failure_threshold: config.circuit_breaker_failure_threshold,
        })
    }

    fn get_next_upstream_index(&self) -> usize {
        self.next_upstream_index.fetch_add(1, Ordering::Relaxed) % self.upstream_addresses.len()
    }

    fn is_upstream_healthy(&self, index: usize) -> bool {
        let state = &self.upstream_states[index];

        if !state.is_open.load(Ordering::Acquire) {
            return true;
        }

        let last_failure = state.last_failure_time_ms.load(Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if now.saturating_sub(last_failure) >= self.recovery_timeout_ms
            && state
                .is_open
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            state.consecutive_failures.store(0, Ordering::Relaxed);
            return true;
        }
        false
    }

    fn find_next_available_upstream_index(&self) -> Option<usize> {
        let mut failed_upstreams = 0;

        loop {
            let index = self.get_next_upstream_index();
            if self.is_upstream_healthy(index) {
                return Some(index);
            } else if failed_upstreams < self.upstream_addresses.len() {
                failed_upstreams += 1;
                continue;
            } else {
                return None;
            }
        }
    }

    fn mark_upstream_success(&self, index: usize) {
        let state = &self.upstream_states[index];
        state.consecutive_failures.store(0, Ordering::Relaxed);
        state.is_open.store(false, Ordering::Release);
    }

    fn mark_upstream_failure(&self, index: usize) {
        let state = &self.upstream_states[index];

        let failures = state.consecutive_failures.fetch_add(1, Ordering::Relaxed);

        if failures + 1 >= self.failure_threshold {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            state.last_failure_time_ms.store(now, Ordering::Relaxed);
            state.is_open.store(true, Ordering::Release);
        }
    }

    async fn forward_request(
        &self,
        request: Request<Incoming>,
    ) -> Result<Response<Incoming>, ForwardError> {
        let upstream_index = self
            .find_next_available_upstream_index()
            .ok_or(ForwardError::NoHealthyUpstreams)?;

        let upstream_address = self
            .upstream_addresses
            .get(upstream_index)
            .expect("Tried to get an upstream address with an index out of bounds.");

        let uri = Uri::builder()
            .scheme("http")
            .authority(upstream_address.clone())
            .path_and_query(
                request
                    .uri()
                    .path_and_query()
                    .cloned()
                    .unwrap_or_else(|| "/".parse().unwrap()),
            )
            .build()
            .expect("Failed to build URI for forwarded request.");

        let (mut parts, body) = request.into_parts();
        parts.uri = uri;

        let upstream_request = Request::from_parts(parts, body);

        let response_or_timeout = tokio::time::timeout(
            self.upstream_timeout_ms,
            self.http_client.request(upstream_request),
        )
        .await;

        match response_or_timeout {
            Ok(Ok(response)) => {
                if response.status().is_success() {
                    self.mark_upstream_success(upstream_index);
                } else {
                    self.mark_upstream_failure(upstream_index);
                }
                Ok(response)
            }
            Ok(Err(_)) => {
                self.mark_upstream_failure(upstream_index);
                Err(ForwardError::UpstreamError)
            }
            Err(_) => {
                self.mark_upstream_failure(upstream_index);
                Err(ForwardError::Timeout)
            }
        }
    }
}

async fn handle_request(
    request: Request<Incoming>,
    load_balancer: Arc<LoadBalancer>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    match load_balancer.forward_request(request).await {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            Ok(Response::from_parts(
                parts,
                Full::new(body.collect().await?.to_bytes()),
            ))
        }
        Err(ForwardError::UpstreamError) => Ok(Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Full::new(Bytes::from_static(b"Bad Gateway")))
            .expect("Failed to build error response.")),
        Err(ForwardError::Timeout) => Ok(Response::builder()
            .status(StatusCode::GATEWAY_TIMEOUT)
            .body(Full::new(Bytes::from_static(b"Gateway Timeout")))
            .expect("Failed to build error response.")),
        Err(ForwardError::NoHealthyUpstreams) => Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Full::new(Bytes::from_static(b"Service Unavailable")))
            .expect("Failed to build error response.")),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load().context("Configuration loading failed")?;
    let address = config.listen_address;
    let load_balancer =
        Arc::new(LoadBalancer::new(config).context("Load balancer initialization failed")?);
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("Failed to bind to address: {}", address))?;

    println!("Load balancer listening on {}", address);

    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let io = TokioIo::new(stream);

        tokio::task::spawn({
            let load_balancer = load_balancer.clone();
            async move {
                let service =
                    service_fn(move |request| handle_request(request, load_balancer.clone()));

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("Error serving connection: {:?}", err);
                }
            }
        });
    }
}
