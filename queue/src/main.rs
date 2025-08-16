use anyhow::{bail, Context, Result};
use config::Config;
use serde::Deserialize;
use std::sync::Mutex;
use std::{collections::VecDeque, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub enum Command {
    Push(Vec<u8>),
    Pop,
}

impl From<Command> for Vec<u8> {
    fn from(value: Command) -> Self {
        match value {
            Command::Push(contents) => {
                let mut result = Vec::with_capacity(contents.len() + 1);
                result.push(0u8);
                result.extend(contents);
                result
            }
            Command::Pop => {
                vec![1u8]
            }
        }
    }
}

pub struct QueueClient {
    stream: TcpStream,
}

impl QueueClient {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn send_command(&mut self, command: Command) -> Result<Response> {
        let bytes: Vec<u8> = command.into();
        self.stream.write_all(bytes.as_slice()).await?;
        let discriminator = self.stream.read_u8().await?;
        match discriminator {
            0 => Ok(Response::Accepted),
            1 => {
                let size = self.stream.read_u16().await?;
                let mut bytes = vec![0u8; size as usize];
                let _ = self.stream.read_exact(bytes.as_mut_slice()).await?;
                Ok(Response::Contents(bytes))
            }
            2 => Ok(Response::QueueEmpty),
            _ => {
                bail!("Invalid response received from the queue.");
            }
        }
    }
}

pub enum Response {
    Accepted,
    Contents(Vec<u8>),
    QueueEmpty,
}

struct CommandManager {
    stream: TcpStream,
}

impl CommandManager {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    async fn get_command(&mut self) -> Result<Command> {
        let discriminator = self.stream.read_u8().await?;
        match discriminator {
            0 => {
                let size = self.stream.read_u16().await?;
                let mut bytes = vec![0u8; size as usize];
                let _ = self.stream.read_exact(bytes.as_mut_slice()).await?;
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
                self.stream.write(&contents).await?;
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
    contents: Mutex<VecDeque<Vec<u8>>>,
}

impl QueueManager {
    fn new() -> Self {
        Self {
            contents: Mutex::new(VecDeque::new()),
        }
    }

    async fn handle_command(&self, command: Command, reader: &mut CommandManager) -> Result<()> {
        match command {
            Command::Push(bytes) => {
                reader.send_response(Response::Accepted).await?;

                let mut contents = self
                    .contents
                    .lock()
                    .expect("Failed to lock queue contents.");
                contents.push_front(bytes);
            }
            Command::Pop => {
                let result = {
                    let mut contents = self
                        .contents
                        .lock()
                        .expect("Failed to lock queue contents.");
                    contents.pop_back()
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let QueueConfig { listen_address } = QueueConfig::load()?;

    let listener = TcpListener::bind(listen_address).await?;
    let queue_manager = Arc::new(QueueManager::new());

    loop {
        let queue_manager = queue_manager.clone();
        let (stream, _) = listener.accept().await?;
        let mut command_manager = CommandManager::new(stream);

        tokio::spawn(async move {
            loop {
                match command_manager.get_command().await {
                    Ok(command) => {
                        if let Err(e) = queue_manager
                            .handle_command(command, &mut command_manager)
                            .await
                        {
                            eprintln!("Error reading command: {}: ", e);
                            let _ = command_manager.shutdown().await;
                            return;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading command: {}: ", e);
                        let _ = command_manager.shutdown().await;
                        return;
                    }
                }
            }
        });
    }
}
