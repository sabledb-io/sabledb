use crate::replication::{
    prepare_std_socket, BytesReader, BytesWriter, ReplicationMessage, TcpStreamBytesReader,
    TcpStreamBytesWriter,
};
use crate::server::ServerOptions;
use crate::{
    io::Archive,
    replication::{StorageUpdates, StorageUpdatesIterItem},
    BatchUpdate, SableError, StorageAdapter, U8ArrayReader,
};

use num_format::{Locale, ToFormattedString};
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use tokio::sync::mpsc::{channel as tokio_channel, error::TryRecvError, Sender as TokioSender};

#[allow(dead_code)]
pub enum ReplClientCommand {
    Shutdown,
}

enum CheckShutdownResult {
    Terminate,
    Timeout,
    Err(String),
}

enum RequestChangesResult {
    /// Close the current connection and attempt to re-connect with the primary
    Reconnect,
    /// Exit the replication thread, do not attempt to reconnect to the primary
    ExitThread,
    /// Command completed successfully
    Success,
    /// Request a full sync from the primary
    FullSync,
}

#[derive(Default)]
pub struct ReplicationClient {}

#[allow(dead_code)]
impl ReplicationClient {
    /// Run replication client on a dedicated thread and return a channel for communicating with it
    pub async fn run(
        &self,
        options: ServerOptions,
        store: StorageAdapter,
    ) -> Result<TokioSender<ReplClientCommand>, SableError> {
        let (tx, mut rx) = tokio_channel::<ReplClientCommand>(100);

        // Spawn a thread to handle the replication
        let _ = std::thread::spawn(move || {
            loop {
                let mut stream = match Self::connect_to_primary(&options) {
                    Err(e) => {
                        crate::error_with_throttling!(300, "Failed to connect to primary. {:?}", e);

                        // Check whether we should attempt to reconnect
                        match Self::check_command_channel(&mut rx) {
                            CheckShutdownResult::Terminate => {
                                tracing::info!(
                                    "Requested to terminate replication client thread. Closing connection with primary"
                                );
                                break; // leave the thread
                            }
                            CheckShutdownResult::Timeout => {}
                            CheckShutdownResult::Err(e) => {
                                tracing::error!(
                                    "Error occurred while reading from channel. {:?}",
                                    e
                                );
                                break; // leave the thread
                            }
                        }
                        std::thread::sleep(std::time::Duration::from_millis(250));
                        continue;
                    }
                    Ok(stream) => stream,
                };

                // hereon: use socket with timeout
                if let Err(e) = prepare_std_socket(&stream) {
                    tracing::error!("Failed to prepare socket. {:?}", e);
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    break;
                }

                // This is the replication main loop:
                // We continuously calling `request_changes` from the primary
                // and store them in our database
                loop {
                    let mut reader = TcpStreamBytesReader::new(&stream);
                    let mut writer = TcpStreamBytesWriter::new(&stream);
                    let result =
                        Self::request_changes(&store, &options, &mut reader, &mut writer, &mut rx);
                    match result {
                        RequestChangesResult::Success => {
                            // Note: if there are no changes, the primary server will stall
                            // the response
                        }
                        RequestChangesResult::Reconnect => {
                            tracing::info!("Closing connection with primary: {:?}", stream);
                            let _ = stream.shutdown(std::net::Shutdown::Both);
                            break;
                        }
                        RequestChangesResult::ExitThread => {
                            tracing::info!("Closing connection with primary: {:?}", stream);
                            let _ = stream.shutdown(std::net::Shutdown::Both);
                            return; // leave the thread
                        }
                        RequestChangesResult::FullSync => {
                            // Try to do a fullsync
                            if let Err(e) = Self::fullsync(&store, &options, &mut stream) {
                                tracing::error!("Fullsync error. {:?}", e);
                                let _ = stream.shutdown(std::net::Shutdown::Both);
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Thread is detached

        Ok(tx)
    }

    fn connect_to_primary(options: &ServerOptions) -> Result<TcpStream, SableError> {
        let repl_config = options.load_replication_config();
        let address = format!("{}:{}", repl_config.ip, repl_config.port);
        tracing::info!("Connecting to primary at: {}", address);

        let addr = address.parse::<SocketAddr>()?;
        let stream = TcpStream::connect(addr)?;
        tracing::info!("Successfully connected to primary at: {}", address);
        Ok(stream)
    }

    fn check_command_channel(
        rx: &mut tokio::sync::mpsc::Receiver<ReplClientCommand>,
    ) -> CheckShutdownResult {
        // Check the channel for commands
        match rx.try_recv() {
            Ok(ReplClientCommand::Shutdown) => {
                tracing::info!("Received request to shutdown replication client");
                CheckShutdownResult::Terminate
            }
            Err(TryRecvError::Empty) => CheckShutdownResult::Timeout,
            Err(e) => CheckShutdownResult::Err(format!(
                "Error occurred while reading from channel. {:?}",
                e
            )),
        }
    }

    /// Perform a fullsync with the primary
    fn fullsync(
        store: &StorageAdapter,
        options: &ServerOptions,
        stream: &mut std::net::TcpStream,
    ) -> Result<(), SableError> {
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_read_timeout(None);

        // Send a "FULL_SYNC" message
        tracing::info!("Sending FULL SYNC message to primary");
        let request = ReplicationMessage::new().with_type(ReplicationMessage::FULL_SYNC);
        let mut buffer = request.to_bytes();
        let mut writer = TcpStreamBytesWriter::new(stream);
        writer.write_message(&mut buffer)?;

        // Read the response
        let mut file_len = vec![0u8; std::mem::size_of::<usize>()];
        stream.read_exact(&mut file_len)?;

        // split the buffer into chunks and read
        const CHUNK_SIZE: usize = 10 << 20; // 10MB
        let count = crate::BytesMutUtils::to_usize(&bytes::BytesMut::from(file_len.as_slice()));

        tracing::info!(
            "Reading file of size: {} bytes",
            count.to_formatted_string(&Locale::en)
        );
        let output_file_name = format!("{}.checkpoint.tar", options.open_params.db_path.display());
        let target_folder_path = format!("{}.checkpoint", options.open_params.db_path.display());
        let mut file = std::fs::File::create(&output_file_name)?;
        crate::io::read_exact(stream, &mut file, count)?;
        tracing::info!(
            "File {} successfully received from primary",
            output_file_name
        );

        // Now that we have the file, extract the tar
        let output_file_name = PathBuf::from(&output_file_name);
        let target_folder_path = PathBuf::from(&target_folder_path);
        let archiver = Archive::default();
        archiver.extract(&output_file_name, &target_folder_path)?;
        tracing::info!(
            "Backup database extracted to: {}",
            target_folder_path.display()
        );

        store.restore_from_checkpoint(&target_folder_path, true)?;
        tracing::info!("Database successfully restored from backup");

        let _ = std::fs::remove_file(&output_file_name);
        let _ = std::fs::remove_dir_all(&target_folder_path);

        // restore the socket state
        prepare_std_socket(stream)?;
        Ok(())
    }

    fn read_replication_message(
        reader: &mut dyn BytesReader,
    ) -> Result<ReplicationMessage, SableError> {
        // Read the request (this is a fixed size request)
        loop {
            let result = match reader.read_message()? {
                None => None,
                Some(bytes) => ReplicationMessage::from_bytes(&bytes),
            };
            match result {
                // And this is why I ❤ Rust...
                Some(req) => break Ok(req),
                None => {
                    continue;
                }
            };
        }
    }

    /// Request a single "change request"
    /// 1. Read the next sequence number to fetch from the primary
    /// 2. Send a `ReplRequest` to the primary
    /// 3. Iterate over the changes and store them in our storage
    /// 4. Update the `changes.seq` file with the new sequence
    /// In various places throughout this function, we check for
    /// shutdown request (this can happen if the server accepted a
    /// request to change role from replica -> primary)
    fn request_changes(
        store: &StorageAdapter,
        options: &ServerOptions,
        reader: &mut impl BytesReader,
        writer: &mut impl BytesWriter,
        rx: &mut tokio::sync::mpsc::Receiver<ReplClientCommand>,
    ) -> RequestChangesResult {
        // Before we start, check for termination request
        match Self::check_command_channel(rx) {
            CheckShutdownResult::Terminate => {
                return RequestChangesResult::ExitThread;
            }
            CheckShutdownResult::Timeout => {}
            CheckShutdownResult::Err(e) => {
                tracing::error!("Error occurred while reading from channel. {:?}", e);
                return RequestChangesResult::ExitThread;
            }
        }

        let sequence_file = options.open_params.db_path.join("changes.seq");
        let Some(sequence_number) = Self::read_next_sequence(sequence_file.clone()) else {
            return RequestChangesResult::ExitThread;
        };
        tracing::info!(
            "Next sequence: {}",
            sequence_number.to_formatted_string(&Locale::en)
        );

        let request = ReplicationMessage::new()
            .with_type(ReplicationMessage::GET_UPDATES_SINCE)
            .with_sequence(sequence_number);

        let mut buffer = request.to_bytes();
        if let Err(e) = writer.write_message(&mut buffer) {
            tracing::error!("Failed to send replication request. {:?}", e);
            return RequestChangesResult::ExitThread;
        };

        // We expect now a `GET_CHANGES_OK` or `GET_CHANGES_ERR` message
        match Self::read_replication_message(reader) {
            Err(e) => {
                tracing::error!("Error reading replication message. {:?}", e);
                return RequestChangesResult::Reconnect;
            }
            Ok(msg) => {
                match msg.message_type {
                    ReplicationMessage::GET_CHANGES_OK => {
                        // fall through
                    }
                    ReplicationMessage::GET_CHANGES_ERR => {
                        // the requested sequence was is not acceptable by the server
                        // do a full sync
                        tracing::info!("Failed to get changes. Requesting fullsync");
                        return RequestChangesResult::FullSync;
                    }
                    other => {
                        tracing::error!("Protocol error: unexpected message type: {:?}", other);
                        return RequestChangesResult::Reconnect;
                    }
                }
            }
        }

        let buffer = loop {
            match reader.read_message() {
                Ok(None) => {
                    // Timeout occurred, this is another good time to check the channel for commands
                    match Self::check_command_channel(rx) {
                        CheckShutdownResult::Terminate => {
                            return RequestChangesResult::ExitThread;
                        }
                        CheckShutdownResult::Timeout => {
                            continue;
                        }
                        CheckShutdownResult::Err(e) => {
                            tracing::error!("Error occurred while reading from channel. {:?}", e);
                            return RequestChangesResult::ExitThread;
                        }
                    }
                }
                Ok(Some(buffer)) => break buffer,
                Err(e) => {
                    tracing::error!("Error reading replication response. {:?}", e);
                    return RequestChangesResult::Reconnect;
                }
            }
        };

        let Some(storage_updates) = StorageUpdates::from_bytes(&buffer) else {
            tracing::error!(
                "Failed to deserialise `StorageUpdats` from bytes. `{:?}`",
                buffer
            );
            return RequestChangesResult::ExitThread;
        };

        tracing::info!("Received changes updates: {}", storage_updates);

        // Keep the "next sequence number" to fetch from the primary
        let sequence_number = storage_updates.end_seq_number;
        let changes_count = storage_updates.changes_count;

        const MAX_BATCH_SIZE: usize = 10_000;
        let mut batch_update = BatchUpdate::with_capacity(MAX_BATCH_SIZE);
        let mut reader = U8ArrayReader::with_buffer(&storage_updates.serialised_data);
        while let Some(change) = storage_updates.next(&mut reader) {
            match change {
                StorageUpdatesIterItem::Put(put_record) => {
                    batch_update.put(put_record.key, put_record.value);
                }
                StorageUpdatesIterItem::Del(delete_record) => {
                    batch_update.delete(delete_record.key);
                }
            }
            if batch_update.len() % MAX_BATCH_SIZE == 0 {
                if let Err(e) = store.apply_batch(&batch_update) {
                    tracing::error!("Failed to apply replication batch into store. {:?}", e);
                    return RequestChangesResult::Reconnect;
                }
                batch_update.clear();
            }
        }

        // make sure all items are applied
        if !batch_update.is_empty() {
            if let Err(e) = store.apply_batch(&batch_update) {
                tracing::error!("Failed to apply replication batch into store. {:?}", e);
                return RequestChangesResult::Reconnect;
            }
            batch_update.clear();
        }

        tracing::info!(
            "Applied {} changes to store, next sequence is: {}",
            changes_count.to_formatted_string(&Locale::en),
            sequence_number.to_formatted_string(&Locale::en)
        );
        Self::write_next_sequence(sequence_file, sequence_number)
    }

    /// Read the next sequence to get from the primary from the file system.
    /// If the file does not exist, return `Some(0)`. Else return the parsed value
    /// or `None` in case of any other error
    fn read_next_sequence(sequence_file: PathBuf) -> Option<u64> {
        match std::fs::read(sequence_file.clone()) {
            Ok(content) => {
                let content = String::from_utf8_lossy(&content).trim().to_string();
                if let Ok(seq) = content.parse::<u64>() {
                    Some(seq)
                } else {
                    tracing::error!(
                        "Failed to parse changes sequence number from path: {}.",
                        sequence_file.display()
                    );
                    None
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(0),
            Err(e) => {
                tracing::error!("Failed to read sequence file content: {:?}.", e);
                None
            }
        }
    }

    /// Write the next sequence to get from the primary to the file system.
    fn write_next_sequence(sequence_file: PathBuf, sequence_number: u64) -> RequestChangesResult {
        let sequence_number = format!("{}", sequence_number);
        if let Err(e) = std::fs::write(sequence_file, sequence_number) {
            tracing::error!("Failed to read sequence file content: {:?}.", e);
            RequestChangesResult::Reconnect
        } else {
            RequestChangesResult::Success
        }
    }
}
//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::PutFlags, StorageOpenParams};
    use bytes::BytesMut;
    const DB_SIZE: usize = 100_000;

    fn create_database(db_name: &str, populate_it: bool) -> Result<StorageAdapter, SableError> {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from(format!("tests/test_{}.db", db_name));
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        let rocks = crate::storage_rocksdb!(open_params.clone());

        if populate_it {
            // put some items
            for i in 0..DB_SIZE {
                let key = format!("key_{}", i);
                let value = format!("value_string_{}", i);
                rocks.put(
                    &BytesMut::from(&key[..]),
                    &BytesMut::from(&value[..]),
                    PutFlags::Override,
                )?;
            }
        }
        Ok(rocks)
    }

    fn verify_all_records_exist(db: &StorageAdapter) -> Result<(), SableError> {
        for i in 0..DB_SIZE {
            let key = format!("key_{}", i);
            let _ = db.get(&BytesMut::from(&key[..]))?.unwrap();
        }
        Ok(())
    }

    #[test]
    fn test_replication_flow() -> Result<(), SableError> {
        // Create 2 databases:
        // Primary database with 100K records and another replication database
        // with no records. After the primary database is populated, trigger
        // the `ReplicationClient::request_changes` function which
        // requests the primary to get all changes since last change (for the
        // test purpose, it is always set to 0). Once the changes are passed,
        // the replica stores them into `replica_db`
        let primary_db = create_database("replication_primary", true)?;
        let replica_db = create_database("replication_replica", false)?;

        let mut writer = SimpleBytesWriter::default();
        let mut reader = StorageUpdatesBytesReader::default();
        reader.add_response(
            ReplicationMessage::new()
                .with_type(ReplicationMessage::GET_CHANGES_OK)
                .to_bytes(),
        );
        reader.add_response(primary_db.storage_updates_since(0, None, None)?.to_bytes());

        let (_tx, mut rx) = tokio_channel::<ReplClientCommand>(100);

        let mut server_options = ServerOptions::default();
        server_options.open_params = replica_db.open_params().clone();
        ReplicationClient::request_changes(
            &replica_db,
            &server_options,
            &mut reader,
            &mut writer,
            &mut rx,
        );

        // Ensure that all record exist in the replication db
        verify_all_records_exist(&replica_db)?;
        Ok(())
    }

    // Mocks used for this test
    #[derive(Default)]
    struct ReplRequestBytesReader {}

    #[derive(Default)]
    struct StorageUpdatesBytesReader {
        buffers: std::collections::VecDeque<BytesMut>,
    }

    #[derive(Default)]
    struct SimpleBytesWriter {
        pub buffers: Vec<BytesMut>,
    }

    impl BytesWriter for SimpleBytesWriter {
        fn write_message(&mut self, buffer: &mut BytesMut) -> Result<(), SableError> {
            self.buffers.push(buffer.clone());
            Ok(())
        }
    }

    impl BytesReader for ReplRequestBytesReader {
        fn read_message(&mut self) -> Result<Option<BytesMut>, SableError> {
            let repl_request = ReplicationMessage::new()
                .with_type(ReplicationMessage::GET_UPDATES_SINCE)
                .with_sequence(0);
            Ok(Some(repl_request.to_bytes()))
        }
    }

    impl StorageUpdatesBytesReader {
        pub fn add_response(&mut self, buf: BytesMut) {
            self.buffers.push_back(buf);
        }
    }

    impl BytesReader for StorageUpdatesBytesReader {
        fn read_message(&mut self) -> Result<Option<BytesMut>, SableError> {
            Ok(self.buffers.pop_front())
        }
    }
}
