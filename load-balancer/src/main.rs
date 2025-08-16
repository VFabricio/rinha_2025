use anyhow::{Context, Result};
use config::Config;
use core::ops::DerefMut;
use serde::Deserialize;
use shared::{Connection, ConnectionPool};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::RwLock,
    task::JoinSet 
};
use tokio_splice::zero_copy_bidirectional;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, Deserialize)]
struct AppConfig {
    connections_per_upstream: usize,
    listen_address: SocketAddr,
    upstream_addresses: Vec<String>,
}

impl AppConfig {
    fn load() -> Result<Self> {
        Config::builder()
            .add_source(config::File::with_name("settings").required(false))
            .add_source(
                config::Environment::with_prefix("LB")
                    .list_separator(",")
                    .with_list_parse_key("upstream_addresses")
                    .try_parsing(true),
            )
            .build()
            .context("Failed to build configuration.")?
            .try_deserialize()
            .context(
                "Failed to deserialize configuration - check that all required fields are set.",
            )
    }
}

#[derive(Eq, PartialEq)]
enum UpstreamState {
    Healthy,
    Degraded,
}

struct Upstream {
    pool: ConnectionPool,
    state: UpstreamState,
}

impl Upstream {
    async fn new(address: SocketAddr, pool_size: usize) -> Result<Self> {
        let pool = ConnectionPool::new(address, pool_size).await?;
        Ok(Self {
            pool,
            state: UpstreamState::Healthy,
        })
    }
}

struct LoadBalancer {
    upstreams: Vec<Upstream>,
    current: AtomicUsize,
}

impl LoadBalancer {
    async fn new(addresses: Vec<SocketAddr>, pool_size: usize) -> Result<Self> {
        let mut join_set = JoinSet::new();

        for address in addresses {
            join_set.spawn(async move { Upstream::new(address, pool_size).await });
        }

        let mut upstreams = vec![];
        for upstream in join_set.join_all().await {
            upstreams.push(upstream?);
        }

        Ok(Self {
            upstreams,
            current: AtomicUsize::new(0),
        })
    }

    fn advance(&self) {
        let current = self.current.load(Ordering::Acquire);
        self.current
            .store((current + 1) % self.upstreams.len(), Ordering::Release);
    }

    fn get_connection(&self) -> Option<&RwLock<Connection>> {
        let mut failed_upstreams = 0;
        let total_upstreams = self.upstreams.len();
        while failed_upstreams < total_upstreams {
            let current = self.current.load(Ordering::Acquire);
            {
                self.advance();
            }
            let upstream = &self.upstreams[current];

            if upstream.state == UpstreamState::Degraded {
                failed_upstreams += 1;
                continue;
            }
            if let Some(connection) = upstream.pool.try_get_connection() {
                return Some(connection);
            } else {
                failed_upstreams += 1;
                eprintln!("Upstream {} has exhausted its connection pool.", current);
            }
        }
        None
    }

    async fn handle_stream(&self, stream: &mut TcpStream) {
        if let Some(connection) = self.get_connection() {
            let mut connection = connection.write().await;
            if let Ok(_) = zero_copy_bidirectional(stream, connection.deref_mut()).await {
                connection.set_available();
            } else {
                connection.set_failed();
            }
        } else {
            eprintln!("Cannot handle connection because all upstream are degraded.");
            let _ = stream.shutdown().await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let AppConfig {
        connections_per_upstream,
        listen_address,
        upstream_addresses,
    } = AppConfig::load().context("Configuration loading failed")?;

    let listener = TcpListener::bind(listen_address).await?;

    println!("Load balancer listening on {}.", listen_address);

    let mut resolved_addresses = vec![];

    for address in upstream_addresses {
        let address = address
            .to_socket_addrs()?
            .next()
            .context("Invalid address.")?;
        resolved_addresses.push(address);
    }

    sleep(Du )

    let load_balancer =
        Arc::new(LoadBalancer::new(resolved_addresses, connections_per_upstream).await?);

    loop {
        let (mut stream, _) = listener.accept().await?;
        let load_balancer = load_balancer.clone();

        tokio::spawn(async move {
            load_balancer.handle_stream(&mut stream).await;
        });
    }
}
