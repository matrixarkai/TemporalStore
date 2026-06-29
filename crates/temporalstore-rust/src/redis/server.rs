use std::io::{self, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::types::{ExecuteRequest, ShardId};
use crate::TemporalStoreClient;

use super::{execute_redis_command_with_state, read_command, RedisCommandState};

pub fn serve_redis_proxy(addr: &str, proxy_addr: String, shard_id: ShardId) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    let client = Arc::new(TemporalStoreClient::new(proxy_addr));
    for stream in listener.incoming() {
        let stream = stream?;
        let client = Arc::clone(&client);
        thread::spawn(move || {
            let _ = handle_stream(stream, &client, shard_id);
        });
    }
    Ok(())
}

fn handle_stream(
    mut stream: TcpStream,
    client: &TemporalStoreClient,
    shard_id: ShardId,
) -> io::Result<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut state = RedisCommandState::default();
    while let Some(args) = read_command(&mut reader)? {
        let response = execute_redis_command_with_state(args, shard_id, &mut state, |command| {
            client
                .execute(ExecuteRequest { shard_id, command })
                .map_err(|err| err.to_string())
                .and_then(|response| {
                    if response.status.ok {
                        Ok(response.response)
                    } else {
                        Err(format!(
                            "{} {}",
                            response.status.code, response.status.message
                        ))
                    }
                })
        });
        stream.write_all(&response.encode())?;
        stream.flush()?;
    }
    Ok(())
}
