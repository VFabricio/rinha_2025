use anyhow::{Context, Result};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use std::fs::File;
use std::io::Write;

#[derive(Debug)]
pub struct Payment {
    pub correlation_id: String,
    pub amount_cents: u64,
    pub timestamp: u64,
}

impl Payment {
    pub fn parse(data: &[u8]) -> Result<Self, anyhow::Error> {
        if data.is_empty() {
            return Err(anyhow::anyhow!("Empty payment data"));
        }

        let id_len = data[0] as usize;
        if data.len() < 1 + id_len + 8 + 8 {
            return Err(anyhow::anyhow!("Invalid payment data length"));
        }

        let correlation_id = String::from_utf8(data[1..1 + id_len].to_vec())
            .context("Invalid UTF-8 in correlation ID")?;

        let amount_bytes: [u8; 8] = data[1 + id_len..1 + id_len + 8]
            .try_into()
            .context("Invalid amount data")?;
        let amount_cents = u64::from_be_bytes(amount_bytes);

        let timestamp_bytes: [u8; 8] = data[1 + id_len + 8..1 + id_len + 16]
            .try_into()
            .context("Invalid timestamp data")?;
        let timestamp = u64::from_be_bytes(timestamp_bytes);

        Ok(Payment {
            correlation_id,
            amount_cents,
            timestamp,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut binary_payment = Vec::with_capacity(1 + self.correlation_id.len() + 8 + 8);
        binary_payment.push(self.correlation_id.len() as u8);
        binary_payment.extend_from_slice(self.correlation_id.as_bytes());
        binary_payment.extend_from_slice(&self.amount_cents.to_be_bytes());
        binary_payment.extend_from_slice(&self.timestamp.to_be_bytes());
        binary_payment
    }
}

#[derive(Debug)]
pub struct PaymentResult {
    pub timestamp: u64,
    pub amount_cents: u64,
    pub processor: u8,
}

impl PaymentResult {
    pub fn serialize(&self) -> [u8; 17] {
        let mut result = [0u8; 17];
        result[0..8].copy_from_slice(&self.timestamp.to_be_bytes());
        result[8..16].copy_from_slice(&self.amount_cents.to_be_bytes());
        result[16] = self.processor;
        result
    }
}

pub struct Queue {
    connection: MultiplexedConnection,
    queue: String,
}

impl Queue {
    pub async fn new(url: &str, queue: &str) -> Result<Self, anyhow::Error> {
        let client = redis::Client::open(url).context("Failed to create Redis client")?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to connect to Redis.")?;
        Ok(Self {
            connection,
            queue: queue.to_owned(),
        })
    }

    pub async fn get_payment(&mut self) -> Result<Payment, anyhow::Error> {
        let (_, data) = self
            .connection
            .brpop::<_, (String, Vec<u8>)>(&self.queue, 1.0)
            .await?;
        let payment = Payment::parse(&data)?;
        Ok(payment)
    }

    pub async fn get_queue_size(&mut self) -> Result<usize, anyhow::Error> {
        let size = self.connection.llen::<_, usize>(&self.queue).await?;
        Ok(size)
    }

    pub async fn put_payment(&self, payment: &Payment) -> Result<(), anyhow::Error> {
        let binary_payment = payment.serialize();
        let mut connection = self.connection.clone();

        connection
            .lpush::<_, _, ()>(&self.queue, binary_payment)
            .await?;
        Ok(())
    }
}

pub struct Database {
    result_directory: String,
    worker_files: Vec<Option<File>>,
}

impl Database {
    pub fn new(result_directory: &str) -> Result<Self> {
        std::fs::create_dir_all(result_directory).context("Failed to create result directory")?;

        Ok(Self {
            result_directory: result_directory.to_string(),
            worker_files: Vec::new(),
        })
    }

    pub fn create_files(&mut self, worker_count: usize) -> Result<()> {
        self.worker_files.clear();
        self.worker_files.reserve(worker_count);

        for worker_id in 0..worker_count {
            let result_file_path = format!("{}/worker_{}.bin", self.result_directory, worker_id);
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&result_file_path)
                .context("Failed to open result file")?;
            self.worker_files.push(Some(file));
        }

        Ok(())
    }

    pub fn log_payment_result(&self, worker_id: usize, result: &PaymentResult) -> Result<()> {
        let file = self
            .worker_files
            .get(worker_id)
            .and_then(|f| f.as_ref())
            .ok_or_else(|| {
                anyhow::anyhow!("Files not created or invalid worker_id: {}", worker_id)
            })?;

        use std::io::Write;
        let mut file_ref = file;
        let bytes = result.serialize();
        file_ref
            .write_all(&bytes)
            .context("Failed to write payment result")?;
        file_ref.flush().context("Failed to flush payment result")?;
        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<PaymentResult>> {
        let mut results = Vec::new();

        let entries =
            std::fs::read_dir(&self.result_directory).context("Failed to read result directory")?;

        for entry in entries {
            let entry = entry.context("Failed to read directory entry")?;
            let file_name = entry.file_name();

            if let Some(name) = file_name.to_str() {
                if name.starts_with("worker_") && name.ends_with(".bin") {
                    let file_path = entry.path();

                    if let Ok(data) = std::fs::read(&file_path) {
                        let mut offset = 0;
                        while offset + 17 <= data.len() {
                            let timestamp =
                                u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
                            let amount_cents = u64::from_be_bytes(
                                data[offset + 8..offset + 16].try_into().unwrap(),
                            );
                            let processor = data[offset + 16];

                            results.push(PaymentResult {
                                timestamp,
                                amount_cents,
                                processor,
                            });

                            offset += 17;
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn purge_all(&self) -> Result<usize> {
        let entries =
            std::fs::read_dir(&self.result_directory).context("Failed to read result directory")?;

        let mut count = 0;
        for entry in entries {
            let entry = entry.context("Failed to read directory entry")?;
            let file_name = entry.file_name();

            if let Some(name) = file_name.to_str() {
                if name.starts_with("worker_") && name.ends_with(".bin") {
                    let file_path = entry.path();
                    std::fs::write(&file_path, b"")
                        .with_context(|| format!("Failed to truncate file: {:?}", file_path))?;
                    count += 1;
                }
            }
        }

        Ok(count)
    }
}
