use anyhow::{bail, Context, Result};
use config::Config;
use queue::{Command, Response};
use serde::Deserialize;
use std::io::ErrorKind;
use std::sync::Mutex;
use std::{collections::VecDeque, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct CommandManager {
    stream: TcpStream,
}

impl CommandManager {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    async fn get_command(&mut self) -> Result<Command> {
        let discriminator = self
            .stream
            .read_u8()
            .await
            .inspect_err(|e| eprintln!("Discriminator error: {}.", e))
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
                    .read_exact(bytes.as_mut_slice())
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

    let listener = TcpListener::bind(&listen_address).await?;

    println!("Queue listening on {}.", listen_address);

    let queue_manager = Arc::new(QueueManager::new());

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
                            eprintln!("Remote {} disconnected.", remote);
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
