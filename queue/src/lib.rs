use anyhow::{bail, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub enum Command {
    Push(Vec<u8>),
    Pop,
}

impl From<Command> for Vec<u8> {
    fn from(value: Command) -> Self {
        match value {
            Command::Push(contents) => {
                let mut result = Vec::with_capacity(contents.len() + 3);
                result.push(0u8);
                result.extend((contents.len() as u16).to_be_bytes());
                result.extend(contents);
                result
            }
            Command::Pop => {
                vec![1u8]
            }
        }
    }
}

pub enum Response {
    Accepted,
    Contents(Vec<u8>),
    QueueEmpty,
}

pub struct QueueClient<'a, S>
where
    S: AsyncReadExt + AsyncWriteExt,
{
    stream: &'a mut S,
}

impl<'a, S: AsyncReadExt + AsyncWriteExt + Unpin> QueueClient<'a, S> {
    pub fn new(stream: &'a mut S) -> Self {
        Self { stream }
    }

    pub async fn send_command(&mut self, command: Command) -> Result<Response> {
        let bytes: Vec<u8> = command.into();
        self.stream.write_all(bytes.as_slice()).await?;
        self.stream.flush().await?;
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
