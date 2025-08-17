use anyhow::{bail, Context, Result};
use config::Config;
use queue::{Command, Response};
use serde::Deserialize;
use std::io::{ErrorKind, SeekFrom};
use std::sync::RwLock;
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc};
use tokio::io::{AsyncSeekExt, BufStream};
use tokio::time::sleep;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::Mutex,
};

struct CommandManager {
    stream: UnixStream,
}

impl CommandManager {
    fn new(stream: UnixStream) -> Self {
        Self { stream }
    }

    async fn get_command(&mut self) -> Result<Command> {
        let discriminator = self
            .stream
            .read_u8()
            .await
            .context("Error reading discriminator.")?;
        match discriminator {
            0 => {
                let size = self
                    .stream
                    .read_u16()
                    .await
                    .context("Error reading command size.")?;
                let mut bytes = vec![0u8; size as usize];
                let _ = self
                    .stream
                    .read_exact(&mut bytes)
                    .await
                    .context("Error reading push contents.")?;
                Ok(Command::Push(bytes))
            }
            1 => Ok(Command::Pop),
            _ => {
                bail!("Unknown command.");
            }
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown().await?;
        Ok(())
    }

    async fn send_response(&mut self, response: Response) -> Result<()> {
        match response {
            Response::Accepted => {
                self.stream.write_u8(0u8).await?;
            }
            Response::Contents(contents) => {
                self.stream.write_u8(1u8).await?;
                self.stream.write_u16(contents.len() as u16).await?;
                self.stream.write_all(&contents).await?;
            }
            Response::QueueEmpty => {
                self.stream.write_u8(2u8).await?;
            }
        }
        let _ = self.stream.flush().await;
        Ok(())
    }
}

#[derive(Deserialize)]
struct QueueConfig {
    listen_address: String,
    snapshot_frequency_seconds: u64,
}

impl QueueConfig {
    fn load() -> Result<Self> {
        Config::builder()
            .add_source(config::File::with_name("settings").required(false))
            .add_source(config::Environment::with_prefix("QUEUE"))
            .build()
            .context("Failed to build configuration.")?
            .try_deserialize()
            .context(
                "Failed to deserialize configuration - check that all required fields are set.",
            )
    }
}

struct QueueManager {
    contents: RwLock<VecDeque<Vec<u8>>>,
}

impl QueueManager {
    fn new(initial_state: VecDeque<Vec<u8>>) -> Self {
        Self {
            contents: RwLock::new(initial_state),
        }
    }

    async fn handle_command(&self, command: Command, reader: &mut CommandManager) -> Result<()> {
        match command {
            Command::Push(bytes) => {
                reader.send_response(Response::Accepted).await?;

                let mut contents = self
                    .contents
                    .write()
                    .expect("Failed to lock queue contents.");
                contents.push_back(bytes);
            }
            Command::Pop => {
                let result = {
                    let mut contents = self
                        .contents
                        .write()
                        .expect("Failed to lock queue contents.");
                    contents.pop_front()
                };
                if let Some(bytes) = result {
                    reader.send_response(Response::Contents(bytes)).await?;
                } else {
                    reader.send_response(Response::QueueEmpty).await?;
                }
            }
        };

        Ok(())
    }

    async fn dump(&self) -> VecDeque<Vec<u8>> {
        let contents = self
            .contents
            .read()
            .expect("Failed to lock queue contents for reading.");
        contents.clone()
    }
}

static DATA_FILE: &str = "data.bin";

struct PersistenceManager {
    file: Mutex<BufStream<File>>,
}

impl PersistenceManager {
    async fn new() -> Result<Self> {
        let file = Mutex::new(BufStream::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(DATA_FILE)
                .await?,
        ));
        Ok(Self { file })
    }

    async fn read_all(&self) -> Result<VecDeque<Vec<u8>>> {
        let mut result = VecDeque::new();
        let mut file = self.file.lock().await;
        loop {
            let size = match file.read_u16().await {
                Ok(s) => s,
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };
            let mut bytes = vec![0u8; size as usize];
            file.read_exact(&mut bytes).await?;
            result.push_back(bytes);
        }
        Ok(result)
    }

    async fn write_all(&self, data: VecDeque<Vec<u8>>) -> Result<()> {
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0)).await?;
        for bytes in &data {
            file.write_u16(bytes.len() as u16).await?;
            file.write_all(bytes).await?;
        }
        let position = file.stream_position().await?;
        file.flush().await?;
        file.get_ref().set_len(position).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let QueueConfig {
        listen_address,
        snapshot_frequency_seconds,
    } = QueueConfig::load()?;

    if std::path::Path::new(&listen_address).exists() {
        std::fs::remove_file(&listen_address)?;
    }

    let listener = UnixListener::bind(&listen_address)?;

    println!("Queue listening on {}.", listen_address);

    let persistence_manager = PersistenceManager::new().await?;
    let contents = persistence_manager.read_all().await?;

    let queue_manager = Arc::new(QueueManager::new(contents));
    let snapshotting_queue_manager = queue_manager.clone();

    tokio::spawn(async move {
        loop {
            let contents = snapshotting_queue_manager.dump().await;
            println!("Snapshotting queue contents: {} items.", contents.len());
            if let Err(e) = persistence_manager.write_all(contents).await {
                eprint!("Error saving queue snapshot: {}.", e)
            };
            sleep(Duration::from_secs(snapshot_frequency_seconds)).await;
        }
    });

    loop {
        let queue_manager = queue_manager.clone();
        let (stream, remote) = listener.accept().await?;

        let mut command_manager = CommandManager::new(stream);

        tokio::spawn(async move {
            loop {
                match command_manager.get_command().await {
                    Ok(command) => {
                        if let Err(e) = queue_manager
                            .handle_command(command, &mut command_manager)
                            .await
                        {
                            eprintln!("Error handling command: {}: ", e);
                            let _ = command_manager.shutdown().await;
                            return;
                        }
                    }
                    Err(e) => {
                        let is_eof = e
                            .downcast_ref::<std::io::Error>()
                            .map(|e| e.kind() == ErrorKind::UnexpectedEof)
                            .unwrap_or(false);

                        if is_eof {
                            eprintln!("Remote {:?} disconnected.", remote);
                        } else {
                            eprintln!("Error reading command: {}: ", e);
                        }
                        let _ = command_manager.shutdown().await;
                        return;
                    }
                }
            }
        });
    }
}
