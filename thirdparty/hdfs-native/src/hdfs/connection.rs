use std::collections::{HashMap, VecDeque};
use std::default::Default;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use crc::{CRC_32_ISCSI, CRC_32_ISO_HDLC, Crc, Table};
use log::{debug, warn};
use prost::Message;
use socket2::SockRef;
use tokio::runtime::Handle;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::{io::AsyncWriteExt, net::TcpStream, task::JoinHandle};
use uuid::Uuid;

use crate::common::config::Configuration;
use crate::proto::common::TokenProto;
use crate::proto::common::rpc_response_header_proto::RpcStatusProto;
use crate::proto::hdfs::{DataEncryptionKeyProto, DatanodeIdProto};
use crate::proto::{common, hdfs};
use crate::security::sasl::{
    SaslDatanodeConnection, SaslDatanodeReader, SaslDatanodeWriter, negotiate_sasl_session,
};
use crate::security::sasl::{SaslReader, SaslWriter};
use crate::security::user::UserInfo;
use crate::{HdfsError, Result};

const PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
const DATA_TRANSFER_VERSION: u16 = 28;
const MAX_PACKET_HEADER_SIZE: usize = 33;
const DATANODE_CACHE_SHARDS: usize = 16;

const CRC32: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISO_HDLC);
const CRC32C: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISCSI);

#[inline]
fn checksum_crc32c(data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        return crc32c::crc32c(data);
    }

    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        return crc32c::crc32c(data);
    }

    // Preserve the existing slicing-by-16 implementation on CPUs without a
    // hardware CRC32C instruction.
    CRC32C.checksum(data)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn datanode_url(datanode_id: &DatanodeIdProto, config: &Configuration) -> String {
    let host = if config.use_datanode_hostname() {
        &datanode_id.host_name
    } else {
        &datanode_id.ip_addr
    };
    format!("{}:{}", host, datanode_id.xfer_port)
}

// Connect to a remote host and return a TcpStream with standard options we want
async fn connect(addr: &str, handle: &Handle) -> Result<TcpStream> {
    let addr = addr.to_string();
    // Spawn a task to create the TcpStream so it captures the tokio runtime in case we
    // are not called from one
    let stream = handle.spawn(TcpStream::connect(addr)).await.unwrap()?;
    stream.set_nodelay(true)?;

    let sf = SockRef::from(&stream);
    sf.set_keepalive(true)?;

    Ok(stream)
}

#[derive(Debug)]
pub(crate) struct AlignmentContext {
    state_id: i64,
    router_federated_state: Option<HashMap<String, i64>>,
}

impl AlignmentContext {
    fn update(
        &mut self,
        state_id: Option<i64>,
        router_federated_state: Option<Vec<u8>>,
    ) -> Result<()> {
        if let Some(new_state_id) = state_id {
            self.state_id = i64::max(new_state_id, self.state_id)
        }

        if let Some(new_router_state) = router_federated_state {
            let new_map = hdfs::RouterFederatedStateProto::decode(Bytes::from(new_router_state))?
                .namespace_state_ids;

            let current_map = if let Some(cur) = self.router_federated_state.as_mut() {
                cur
            } else {
                self.router_federated_state = Some(HashMap::new());
                self.router_federated_state.as_mut().unwrap()
            };

            for (key, value) in new_map.into_iter() {
                current_map.insert(
                    key.clone(),
                    i64::max(value, *current_map.get(&key).unwrap_or(&i64::MIN)),
                );
            }
        }

        Ok(())
    }

    fn encode_router_state(&self) -> Option<Vec<u8>> {
        self.router_federated_state.as_ref().map(|state| {
            hdfs::RouterFederatedStateProto {
                namespace_state_ids: state.clone(),
            }
            .encode_to_vec()
        })
    }
}

impl Default for AlignmentContext {
    fn default() -> Self {
        Self {
            state_id: i64::MIN,
            router_federated_state: None,
        }
    }
}

type CallResult = oneshot::Sender<Result<Bytes>>;

#[derive(Debug)]
pub(crate) struct RpcConnection {
    client_id: Vec<u8>,
    user_info: UserInfo,
    next_call_id: AtomicI32,
    alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
    call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
    sender: mpsc::Sender<Vec<u8>>,
    listener: Option<JoinHandle<()>>,
}

impl RpcConnection {
    pub(crate) async fn connect(
        url: &str,
        alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
        nameservice: Option<&str>,
        effective_user: Option<String>,
        auth: Option<std::sync::Arc<crate::security::ClientAuth>>,
        config: &Configuration,
        handle: &Handle,
    ) -> Result<Self> {
        let client_id = Uuid::new_v4().to_bytes_le().to_vec();
        let next_call_id = AtomicI32::new(0);
        let call_map = Arc::new(Mutex::new(Some(HashMap::new())));

        let mut stream = connect(url, handle).await?;
        stream.write_all("hrpc".as_bytes()).await?;
        // Current version
        stream.write_all(&[9u8]).await?;
        // Service class
        stream.write_all(&[0u8]).await?;
        // Auth protocol
        if config.security_enabled() {
            stream.write_all(&(-33i8).to_be_bytes()).await?;
        } else {
            stream.write_all(&(0i8).to_be_bytes()).await?;
        }

        let service = nameservice
            .map(|ns| format!("ha-hdfs:{ns}"))
            .unwrap_or(url.to_string());
        let (user_info, reader, writer) =
            negotiate_sasl_session(stream, &service, config, effective_user, auth).await?;
        let (sender, receiver) = mpsc::channel::<Vec<u8>>(1000);

        let mut conn = RpcConnection {
            client_id,
            user_info,
            next_call_id,
            alignment_context,
            call_map,
            listener: None,
            sender,
        };

        conn.start_sender(receiver, writer, handle);

        let context_header = conn
            .get_connection_header(-3, -1)
            .encode_length_delimited_to_vec();
        let context_msg = conn
            .get_connection_context()
            .encode_length_delimited_to_vec();
        conn.write_messages(&[&context_header, &context_msg])
            .await?;
        let listener = conn.start_listener(reader, handle)?;
        conn.listener = Some(listener);

        Ok(conn)
    }

    fn start_sender(
        &mut self,
        mut rx: mpsc::Receiver<Vec<u8>>,
        mut writer: SaslWriter,
        handle: &Handle,
    ) {
        handle.spawn(async move {
            while let Some(msg) = rx.recv().await {
                match writer.write_all(&msg).await {
                    Ok(_) => (),
                    Err(_) => break,
                }
            }
        });
    }

    fn start_listener(&mut self, reader: SaslReader, handle: &Handle) -> Result<JoinHandle<()>> {
        let call_map = Arc::clone(&self.call_map);
        let alignment_context = self.alignment_context.clone();
        let listener = handle.spawn(async move {
            RpcListener::new(call_map, reader, alignment_context)
                .start()
                .await;
        });
        Ok(listener)
    }

    fn get_next_call_id(&self) -> i32 {
        self.next_call_id.fetch_add(1, Ordering::SeqCst)
    }

    fn get_connection_header(
        &self,
        call_id: i32,
        retry_count: i32,
    ) -> common::RpcRequestHeaderProto {
        let (state_id, router_federated_state) =
            if let Some(context) = self.alignment_context.as_ref() {
                let context = context.lock().unwrap();
                (Some(context.state_id), context.encode_router_state())
            } else {
                (None, None)
            };

        common::RpcRequestHeaderProto {
            rpc_kind: Some(common::RpcKindProto::RpcProtocolBuffer as i32),
            // RPC_FINAL_PACKET
            rpc_op: Some(0),
            call_id,
            client_id: self.client_id.clone(),
            retry_count: Some(retry_count),
            state_id,
            router_federated_state,
            ..Default::default()
        }
    }

    fn get_connection_context(&self) -> common::IpcConnectionContextProto {
        let user_info = common::UserInformationProto {
            effective_user: self.user_info.effective_user.clone(),
            real_user: self.user_info.real_user.clone(),
        };

        let context = common::IpcConnectionContextProto {
            protocol: Some(PROTOCOL.to_string()),
            user_info: Some(user_info),
        };

        debug!("Connection context: {:?}", context);
        context
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.listener
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    pub(crate) async fn write_messages(&self, messages: &[&[u8]]) -> Result<()> {
        let mut size = 0u32;
        for msg in messages.iter() {
            size += msg.len() as u32;
        }

        let mut buf: Vec<u8> = Vec::with_capacity(size as usize + 4);

        buf.extend(size.to_be_bytes());
        for msg in messages.iter() {
            buf.extend(*msg);
        }

        let _ = self.sender.send(buf).await;

        Ok(())
    }

    pub(crate) async fn call(
        &self,
        method_name: &str,
        message: &[u8],
    ) -> Result<oneshot::Receiver<Result<Bytes>>> {
        let call_id = self.get_next_call_id();
        let conn_header = self.get_connection_header(call_id, 0);

        debug!("RPC connection header: {:?}", conn_header);

        let conn_header_buf = conn_header.encode_length_delimited_to_vec();

        let msg_header = common::RequestHeaderProto {
            method_name: method_name.to_string(),
            declaring_class_protocol_name: PROTOCOL.to_string(),
            client_protocol_version: 1,
        };
        debug!("RPC request header: {:?}", msg_header);

        let header_buf = msg_header.encode_length_delimited_to_vec();

        let (sender, receiver) = oneshot::channel::<Result<Bytes>>();

        {
            let mut map = self.call_map.lock().unwrap();
            match map.as_mut() {
                Some(m) => {
                    m.insert(call_id, sender);
                }
                None => {
                    return Err(HdfsError::IOError(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "RPC listener disconnected",
                    )));
                }
            }
        }

        self.write_messages(&[&conn_header_buf, &header_buf, message])
            .await?;

        Ok(receiver)
    }
}

struct RpcListener {
    call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
    reader: SaslReader,
    alive: bool,
    alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
}

impl RpcListener {
    fn new(
        call_map: Arc<Mutex<Option<HashMap<i32, CallResult>>>>,
        reader: SaslReader,
        alignment_context: Option<Arc<Mutex<AlignmentContext>>>,
    ) -> Self {
        RpcListener {
            call_map,
            reader,
            alive: true,
            alignment_context,
        }
    }

    async fn start(&mut self) {
        loop {
            if let Err(error) = self.read_response().await {
                match &error {
                    HdfsError::IOError(e) if e.kind() == ErrorKind::UnexpectedEof => {}
                    e => {
                        warn!("RPC listener error: {:?}", e);
                    }
                }
                break;
            }
        }
        self.alive = false;

        if let Some(map) = self.call_map.lock().unwrap().take() {
            for (_, call) in map {
                let _ = call.send(Err(HdfsError::IOError(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "RPC listener disconnected",
                ))));
            }
        }
    }

    async fn read_response(&mut self) -> Result<()> {
        // Read the size of the message
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).await?;
        let msg_length = u32::from_be_bytes(buf);

        // Read the whole message
        let mut buf = BytesMut::zeroed(msg_length as usize);
        self.reader.read_exact(&mut buf).await?;

        let mut bytes = buf.freeze();
        let rpc_response = common::RpcResponseHeaderProto::decode_length_delimited(&mut bytes)?;

        debug!("RPC header response: {:?}", rpc_response);

        #[cfg(feature = "integration-test")]
        if crate::test::NAMENODE_RESPONSE_FAULT_INJECTOR.swap(false, Ordering::SeqCst) {
            return Err(HdfsError::IOError(std::io::Error::new(
                ErrorKind::ConnectionAborted,
                "Injected NameNode response failure",
            )));
        }

        let call_id = rpc_response.call_id as i32;

        let call = self
            .call_map
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|m| m.remove(&call_id));

        if let Some(call) = call {
            match rpc_response.status() {
                RpcStatusProto::Success => {
                    self.alignment_context
                        .as_ref()
                        .map(|alignment_context| {
                            alignment_context
                                .lock()
                                .unwrap()
                                .update(rpc_response.state_id, rpc_response.router_federated_state)
                        })
                        .transpose()?;

                    let _ = call.send(Ok(bytes));
                }
                RpcStatusProto::Error => {
                    let _ = call.send(Err(HdfsError::RPCError(
                        rpc_response.exception_class_name().to_string(),
                        rpc_response.error_msg().to_string(),
                    )));
                }
                RpcStatusProto::Fatal => {
                    warn!(
                        "RPC fatal error: {}: {}",
                        rpc_response.exception_class_name(),
                        rpc_response.error_msg()
                    );
                    return Err(HdfsError::FatalRPCError(
                        rpc_response.exception_class_name().to_string(),
                        rpc_response.error_msg().to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::enum_variant_names)]
pub(crate) enum Op {
    WriteBlock,
    ReadBlock,
    TransferBlock,
}

impl Op {
    fn value(&self) -> u8 {
        match self {
            Self::WriteBlock => 80,
            Self::ReadBlock => 81,
            Self::TransferBlock => 86,
        }
    }
}

const CHECKSUM_BYTES: usize = 4;
const PACKET_DATA_LENGTH_BYTES: usize = 4;

fn packet_body_length(payload_length: usize) -> Result<usize> {
    payload_length
        .checked_sub(PACKET_DATA_LENGTH_BYTES)
        .ok_or_else(|| {
            HdfsError::DataTransferError(format!("invalid packet payload length: {payload_length}"))
        })
}

fn packet_checksum_length(body_length: usize, data_length: i32) -> Result<usize> {
    let data_length = usize::try_from(data_length).map_err(|_| {
        HdfsError::DataTransferError(format!("invalid packet data length: {data_length}"))
    })?;
    body_length.checked_sub(data_length).ok_or_else(|| {
        HdfsError::DataTransferError(format!(
            "packet data length {data_length} exceeds body length {body_length}"
        ))
    })
}

#[inline]
fn verify_checksums<F>(
    data: &[u8],
    checksums: &[u8],
    bytes_per_checksum: usize,
    calculate: F,
) -> Result<()>
where
    F: Fn(&[u8]) -> u32,
{
    if bytes_per_checksum == 0 {
        return Err(HdfsError::ChecksumError);
    }

    let checksum_count = data.len().div_ceil(bytes_per_checksum);
    let required_checksum_bytes = checksum_count
        .checked_mul(CHECKSUM_BYTES)
        .ok_or(HdfsError::ChecksumError)?;
    if checksums.len() < required_checksum_bytes {
        return Err(HdfsError::ChecksumError);
    }

    for (data_chunk, checksum_chunk) in data
        .chunks(bytes_per_checksum)
        .zip(checksums.chunks_exact(CHECKSUM_BYTES))
    {
        let expected = u32::from_be_bytes([
            checksum_chunk[0],
            checksum_chunk[1],
            checksum_chunk[2],
            checksum_chunk[3],
        ]);
        if calculate(data_chunk) != expected {
            return Err(HdfsError::ChecksumError);
        }
    }
    Ok(())
}

pub(crate) struct ReadPacket {
    pub header: hdfs::PacketHeaderProto,
    checksum: Bytes,
    data: Bytes,
}

impl ReadPacket {
    fn new(header: hdfs::PacketHeaderProto, checksum: Bytes, data: Bytes) -> Self {
        Self {
            header,
            checksum,
            data,
        }
    }

    pub(crate) fn get_data(
        self,
        checksum_info: &Option<hdfs::ReadOpChecksumInfoProto>,
    ) -> Result<Bytes> {
        #[cfg(feature = "integration-test")]
        let checksum_started_at = std::time::Instant::now();
        #[cfg(feature = "integration-test")]
        let mut checksum_verified = false;
        // Verify the checksums if they were requested
        if let Some(info) = checksum_info {
            let bytes_per_checksum = info.checksum.bytes_per_checksum as usize;
            match info.checksum.r#type() {
                hdfs::ChecksumTypeProto::ChecksumCrc32 => {
                    verify_checksums(&self.data, &self.checksum, bytes_per_checksum, |data| {
                        CRC32.checksum(data)
                    })?;
                    #[cfg(feature = "integration-test")]
                    {
                        checksum_verified = true;
                    }
                }
                hdfs::ChecksumTypeProto::ChecksumCrc32c => {
                    verify_checksums(
                        &self.data,
                        &self.checksum,
                        bytes_per_checksum,
                        checksum_crc32c,
                    )?;
                    #[cfg(feature = "integration-test")]
                    {
                        checksum_verified = true;
                    }
                }
                hdfs::ChecksumTypeProto::ChecksumNull => {}
            }
        }
        #[cfg(feature = "integration-test")]
        if checksum_verified {
            crate::test::CHECKSUM_BYTES_VERIFIED
                .fetch_add(self.data.len() as u64, std::sync::atomic::Ordering::Relaxed);
            crate::test::CHECKSUM_VERIFY_NANOS.fetch_add(
                checksum_started_at.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        Ok(self.data)
    }
}

pub(crate) struct WritePacket {
    pub header: hdfs::PacketHeaderProto,
    pub data: BytesMut,
    bytes_per_checksum: usize,
    max_data_size: usize,
}

impl WritePacket {
    pub(crate) fn empty(
        offset: i64,
        seqno: i64,
        bytes_per_checksum: u32,
        max_packet_size: u32,
    ) -> Self {
        let header = hdfs::PacketHeaderProto {
            offset_in_block: offset,
            seqno,
            ..Default::default()
        };

        let num_chunks = Self::max_packet_chunks(bytes_per_checksum, max_packet_size);

        Self {
            header,
            data: BytesMut::with_capacity(num_chunks * bytes_per_checksum as usize),
            bytes_per_checksum: bytes_per_checksum as usize,
            max_data_size: num_chunks * bytes_per_checksum as usize,
        }
    }

    pub(crate) fn set_last_packet(&mut self) {
        self.header.last_packet_in_block = true;
        // Opinionated: always sync block for safety
        self.header.sync_block = Some(true);
    }

    fn max_packet_chunks(bytes_per_checksum: u32, max_packet_size: u32) -> usize {
        if max_packet_size > 0 {
            let data_size = max_packet_size as usize - MAX_PACKET_HEADER_SIZE;
            let chunk_size = bytes_per_checksum as usize + CHECKSUM_BYTES;
            data_size / chunk_size
        } else {
            // Create a packet with a single chunk for appending to a file
            1
        }
    }

    pub(crate) fn write(&mut self, buf: &mut Bytes) {
        self.data
            .put(buf.split_to(usize::min(self.max_data_size - self.data.len(), buf.len())));
        self.header.data_len = self.data.len() as i32;
    }

    pub(crate) fn is_full(&self) -> bool {
        self.data.len() == self.max_data_size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn calculate_checksum(&mut self) -> Bytes {
        if self.data.is_empty() || self.bytes_per_checksum == 0 {
            return Bytes::new();
        }

        let checksum_count = self.data.len().div_ceil(self.bytes_per_checksum);
        let mut checksum = BytesMut::with_capacity(checksum_count * CHECKSUM_BYTES);

        for chunk in self.data.chunks(self.bytes_per_checksum) {
            checksum.put_u32(checksum_crc32c(chunk));
        }

        checksum.freeze()
    }
}

pub(crate) struct DatanodeConnection {
    client_name: String,
    reader: SaslDatanodeReader,
    writer: SaslDatanodeWriter,
    url: String,
    // Present only for read connections when a client-level total connection
    // budget is configured. The permit follows the live TCP connection both
    // while checked out and while retained in the idle cache.
    _total_capacity_permit: Option<TotalConnectionPermit>,
}

impl DatanodeConnection {
    pub(crate) async fn connect(
        datanode_id: &DatanodeIdProto,
        token: &TokenProto,
        encryption_key: Option<DataEncryptionKeyProto>,
        config: &Configuration,
        handle: &Handle,
    ) -> Result<Self> {
        #[cfg(feature = "integration-test")]
        let started_at = std::time::Instant::now();
        let url = datanode_url(datanode_id, config);
        let stream = connect(&url, handle).await?;

        let sasl_connection = SaslDatanodeConnection::create(stream);
        let (reader, writer) = sasl_connection
            .negotiate(datanode_id, token, encryption_key.as_ref(), config)
            .await?;

        let conn = DatanodeConnection {
            client_name: Uuid::new_v4().to_string(),
            reader,
            writer,
            url: url.to_string(),
            _total_capacity_permit: None,
        };
        #[cfg(feature = "integration-test")]
        {
            crate::test::DATANODE_NEW_CONNECTIONS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            crate::test::DATANODE_NEW_CONNECTION_NANOS.fetch_add(
                started_at.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        Ok(conn)
    }

    pub(crate) fn set_total_capacity_permit(&mut self, permit: Option<TotalConnectionPermit>) {
        self._total_capacity_permit = permit;
    }

    pub(crate) async fn send(
        &mut self,
        op: Op,
        message: &impl Message,
    ) -> Result<hdfs::BlockOpResponseProto> {
        self.writer
            .write_all(&DATA_TRANSFER_VERSION.to_be_bytes())
            .await?;
        self.writer.write_all(&[op.value()]).await?;
        self.writer
            .write_all(&message.encode_length_delimited_to_vec())
            .await?;
        self.writer.flush().await?;

        let message = self.reader.read_proto().await?;

        let response = hdfs::BlockOpResponseProto::decode(message)?;
        Ok(response)
    }

    pub(crate) fn build_header(
        &self,
        block: &hdfs::ExtendedBlockProto,
        token: Option<common::TokenProto>,
    ) -> hdfs::ClientOperationHeaderProto {
        let base_header = hdfs::BaseHeaderProto {
            block: block.clone(),
            token,
            ..Default::default()
        };

        hdfs::ClientOperationHeaderProto {
            base_header,
            client_name: self.client_name.clone(),
        }
    }

    pub(crate) async fn read_packet(&mut self) -> Result<ReadPacket> {
        let mut payload_len_buf = [0u8; 4];
        let mut header_len_buf = [0u8; 2];
        self.reader.read_exact(&mut payload_len_buf).await?;
        self.reader.read_exact(&mut header_len_buf).await?;

        let payload_length = u32::from_be_bytes(payload_len_buf) as usize;
        let header_length = u16::from_be_bytes(header_len_buf) as usize;
        let body_length = packet_body_length(payload_length)?;
        let remaining_length = body_length.checked_add(header_length).ok_or_else(|| {
            HdfsError::DataTransferError(format!(
                "packet length overflow: payload={payload_length}, header={header_length}"
            ))
        })?;

        let mut remaining_buf = BytesMut::zeroed(remaining_length);
        self.reader.read_exact(&mut remaining_buf).await?;

        let header =
            hdfs::PacketHeaderProto::decode(remaining_buf.split_to(header_length).freeze())?;

        let checksum_length = packet_checksum_length(body_length, header.data_len)?;
        let checksum = remaining_buf.split_to(checksum_length).freeze();
        let data = remaining_buf.freeze();

        Ok(ReadPacket::new(header, checksum, data))
    }

    pub(crate) async fn send_read_success(&mut self) -> Result<()> {
        let client_read_status = hdfs::ClientReadStatusProto {
            status: hdfs::Status::ChecksumOk as i32,
        };

        self.writer
            .write_all(&client_read_status.encode_length_delimited_to_vec())
            .await?;
        self.writer.flush().await?;

        Ok(())
    }

    pub(crate) fn split(self) -> (DatanodeReader, DatanodeWriter) {
        let reader = DatanodeReader {
            reader: self.reader,
        };
        let writer = DatanodeWriter {
            writer: self.writer,
        };
        (reader, writer)
    }
}

/// A reader half of a Datanode connection used for reading acks during
/// write operations.
pub(crate) struct DatanodeReader {
    reader: SaslDatanodeReader,
}

impl DatanodeReader {
    pub(crate) async fn read_ack(&mut self) -> Result<hdfs::PipelineAckProto> {
        let message = self.reader.read_proto().await?;

        let response = hdfs::PipelineAckProto::decode(message)?;
        Ok(response)
    }
}

/// A write half of a Datanode connection used for writing packets.
pub(crate) struct DatanodeWriter {
    writer: SaslDatanodeWriter,
}

impl DatanodeWriter {
    /// Create a buffer to send to the datanode
    pub(crate) async fn write_packet(&mut self, packet: &mut WritePacket) -> Result<()> {
        let checksum = packet.calculate_checksum();

        let payload_len = (checksum.len() + packet.data.len() + 4) as u32;
        let header_encoded = packet.header.encode_to_vec();

        self.writer.write_all(&payload_len.to_be_bytes()).await?;
        self.writer
            .write_all(&(header_encoded.len() as u16).to_be_bytes())
            .await?;
        self.writer.write_all(&header_encoded).await?;
        self.writer.write_all(&checksum).await?;
        self.writer.write_all(&packet.data).await?;
        self.writer.flush().await?;

        Ok(())
    }
}

struct CachedDatanodeConnection {
    released_at: Instant,
    connection: DatanodeConnection,
    _capacity_permit: OwnedSemaphorePermit,
}

pub(crate) struct TotalConnectionPermit {
    permit: Option<OwnedSemaphorePermit>,
    connection_available: Arc<Notify>,
}

pub(crate) struct DatanodeConnectionAcquisition {
    pub(crate) connection: Option<DatanodeConnection>,
    pub(crate) total_capacity_permit: Option<TotalConnectionPermit>,
}

impl Drop for TotalConnectionPermit {
    fn drop(&mut self) {
        // Rust drops fields after `Drop::drop` returns. Release explicitly so
        // a woken waiter cannot observe the semaphore at capacity and go back
        // to sleep without another notification.
        drop(self.permit.take());
        self.connection_available.notify_one();
    }
}

type DatanodeConnectionCacheEntry = VecDeque<CachedDatanodeConnection>;
type DatanodeConnectionCacheShard = Mutex<HashMap<String, DatanodeConnectionCacheEntry>>;

/// Client-scoped cache of idle DataNode connections.
///
/// This follows Hadoop's `PeerCache` lifecycle: idle peers are bounded by
/// `dfs.client.socketcache.capacity`, expire after
/// `dfs.client.socketcache.expiryMsec`, and stale cached peers are retried only
/// `dfs.client.cached.conn.retry` times. Unlike the Java implementation, the
/// cache is sharded by DataNode so unrelated reads do not contend on one lock.
pub(crate) struct DatanodeConnectionCache {
    shards: Vec<DatanodeConnectionCacheShard>,
    capacity: usize,
    capacity_permits: Arc<Semaphore>,
    max_total: usize,
    total_capacity_permits: Option<Arc<Semaphore>>,
    connection_available: Arc<Notify>,
    expiry: Duration,
    cached_connection_retries: usize,
    eviction_cursor: AtomicUsize,
    cleanup_started: AtomicBool,
    replica_round_robin: bool,
    replica_cursor: AtomicUsize,
    handle: Handle,
}

impl fmt::Debug for DatanodeConnectionCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatanodeConnectionCache")
            .field("capacity", &self.capacity)
            .field("max_total", &self.max_total)
            .field("expiry", &self.expiry)
            .field("cached_connection_retries", &self.cached_connection_retries)
            .finish_non_exhaustive()
    }
}

impl DatanodeConnectionCache {
    pub(crate) fn new(
        capacity: usize,
        max_total: usize,
        expiry: Duration,
        cached_connection_retries: usize,
        replica_round_robin: bool,
        handle: &Handle,
    ) -> Arc<Self> {
        let shard_count = DATANODE_CACHE_SHARDS.min(capacity.max(1));
        Arc::new(Self {
            shards: (0..shard_count)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            capacity,
            capacity_permits: Arc::new(Semaphore::new(capacity)),
            max_total,
            total_capacity_permits: (max_total > 0).then(|| Arc::new(Semaphore::new(max_total))),
            connection_available: Arc::new(Notify::new()),
            expiry,
            cached_connection_retries,
            eviction_cursor: AtomicUsize::new(0),
            cleanup_started: AtomicBool::new(false),
            replica_round_robin,
            replica_cursor: AtomicUsize::new(0),
            handle: handle.clone(),
        })
    }

    pub(crate) fn cached_connection_retries(&self) -> usize {
        self.cached_connection_retries
    }

    pub(crate) fn next_replica_offset(&self) -> Option<usize> {
        self.replica_round_robin
            .then(|| self.replica_cursor.fetch_add(1, Ordering::Relaxed))
    }

    /// Acquire a reusable peer or reserve capacity for a new live DataNode read
    /// connection. A zero total limit preserves hdfs-native's historical
    /// unlimited behavior. Waiters always recheck the target DataNode first so
    /// a freshly returned hot peer is reused instead of immediately evicted.
    pub(crate) async fn acquire(
        &self,
        datanode_id: &hdfs::DatanodeIdProto,
        config: &Configuration,
        allow_cached: bool,
    ) -> DatanodeConnectionAcquisition {
        loop {
            let notified = self.connection_available.notified();
            tokio::pin!(notified);
            // Register before checking either source of capacity. This closes
            // the gap where a connection could be returned between the checks
            // below and the first poll of `notified`.
            let _ = notified.as_mut().enable();
            if allow_cached && let Some(connection) = self.get(datanode_id, config) {
                return DatanodeConnectionAcquisition {
                    connection: Some(connection),
                    total_capacity_permit: None,
                };
            }
            let Some(permits) = self.total_capacity_permits.as_ref() else {
                return DatanodeConnectionAcquisition {
                    connection: None,
                    total_capacity_permit: None,
                };
            };
            if let Ok(permit) = Arc::clone(permits).try_acquire_owned() {
                return DatanodeConnectionAcquisition {
                    connection: None,
                    total_capacity_permit: Some(TotalConnectionPermit {
                        permit: Some(permit),
                        connection_available: Arc::clone(&self.connection_available),
                    }),
                };
            }
            if self.evict_one() {
                continue;
            }
            #[cfg(feature = "integration-test")]
            crate::test::DATANODE_CONNECTION_POOL_WAITS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            notified.await;
        }
    }

    fn shard(&self, url: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        url.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }

    pub(crate) fn get(
        &self,
        datanode_id: &hdfs::DatanodeIdProto,
        config: &Configuration,
    ) -> Option<DatanodeConnection> {
        if self.capacity == 0 {
            #[cfg(feature = "integration-test")]
            crate::test::DATANODE_CONNECTION_CACHE_MISSES
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        }

        let url = datanode_url(datanode_id, config);
        let shard_index = self.shard(&url);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let mut remove_entry = false;
        let result = if let Some(connections) = shard.get_mut(&url) {
            let mut result = None;
            while let Some(cached) = connections.pop_front() {
                if cached.released_at.elapsed() < self.expiry {
                    result = Some(cached.connection);
                    break;
                }
                #[cfg(feature = "integration-test")]
                crate::test::DATANODE_CONNECTION_CACHE_EXPIRED
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            remove_entry = connections.is_empty();
            result
        } else {
            None
        };
        if remove_entry {
            shard.remove(&url);
        }
        drop(shard);

        #[cfg(feature = "integration-test")]
        if result.is_some() {
            crate::test::DATANODE_CONNECTION_CACHE_HITS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            crate::test::DATANODE_CONNECTION_CACHE_MISSES
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    pub(crate) fn release(self: &Arc<Self>, conn: DatanodeConnection) {
        if self.capacity == 0 {
            return;
        }

        let capacity_permit = match Arc::clone(&self.capacity_permits).try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                // If another thread checked out an idle connection after the
                // first permit attempt, there may be no entry left to evict,
                // but its permit is available. Retry either way.
                self.evict_one();
                Arc::clone(&self.capacity_permits).try_acquire_owned().ok()
            }
        };
        let Some(capacity_permit) = capacity_permit else {
            #[cfg(feature = "integration-test")]
            crate::test::DATANODE_CONNECTION_CACHE_REJECTED
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        };

        let shard_index = self.shard(&conn.url);
        lock_unpoisoned(&self.shards[shard_index])
            .entry(conn.url.clone())
            .or_default()
            .push_back(CachedDatanodeConnection {
                released_at: Instant::now(),
                connection: conn,
                _capacity_permit: capacity_permit,
            });
        self.connection_available.notify_one();
        self.start_cleanup();
    }

    fn start_cleanup(self: &Arc<Self>) {
        if self
            .cleanup_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let weak_cache = Arc::downgrade(self);
        let expiry = self.expiry;
        self.handle.spawn(async move {
            let mut interval = tokio::time::interval(expiry);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Tokio intervals tick immediately once. Consume that tick so the
            // first cleanup happens after one full expiry period.
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(cache) = weak_cache.upgrade() else {
                    break;
                };
                cache.remove_expired();
            }
        });
    }

    fn evict_one(&self) -> bool {
        let start = self.eviction_cursor.fetch_add(1, Ordering::Relaxed) % self.shards.len();
        for offset in 0..self.shards.len() {
            let shard_index = (start + offset) % self.shards.len();
            let mut shard = lock_unpoisoned(&self.shards[shard_index]);
            let eviction_url = shard
                .iter()
                .filter_map(|(url, connections)| {
                    connections.front().map(|cached| (url, cached.released_at))
                })
                .min_by_key(|(_, released_at)| *released_at)
                .map(|(url, _)| url.clone());
            let Some(eviction_url) = eviction_url else {
                continue;
            };
            let remove_entry = if let Some(connections) = shard.get_mut(&eviction_url) {
                connections.pop_front();
                connections.is_empty()
            } else {
                false
            };
            if remove_entry {
                shard.remove(&eviction_url);
            }
            return true;
        }
        false
    }

    fn remove_expired(&self) {
        for shard in &self.shards {
            lock_unpoisoned(shard).retain(|_, connections| {
                #[cfg(feature = "integration-test")]
                let original_len = connections.len();
                connections.retain(|cached| cached.released_at.elapsed() < self.expiry);
                #[cfg(feature = "integration-test")]
                crate::test::DATANODE_CONNECTION_CACHE_EXPIRED.fetch_add(
                    (original_len - connections.len()) as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                !connections.is_empty()
            });
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    use prost::Message;
    use tokio::sync::{Notify, Semaphore, mpsc};

    use crate::{
        common::config::Configuration, hdfs::connection::MAX_PACKET_HEADER_SIZE, proto::hdfs,
        security::user::UserInfo,
    };

    use super::{
        AlignmentContext, CRC32, CRC32C, ReadPacket, RpcConnection, TotalConnectionPermit,
        WritePacket, datanode_url, packet_body_length, packet_checksum_length,
    };
    use crate::HdfsError;
    use bytes::{BufMut, Bytes, BytesMut};

    #[test]
    fn test_max_packet_header_size() {
        // Create a dummy header to get its size
        let header = hdfs::PacketHeaderProto {
            sync_block: Some(false),
            ..Default::default()
        };
        // Add 4 bytes for size of whole packet and 2 bytes for size of header
        assert_eq!(MAX_PACKET_HEADER_SIZE, header.encoded_len() + 4 + 2);
    }

    struct PermitCheckingWaker {
        semaphore: Arc<Semaphore>,
        called: AtomicBool,
        permit_was_available: AtomicBool,
    }

    impl Wake for PermitCheckingWaker {
        fn wake(self: Arc<Self>) {
            self.record();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.record();
        }
    }

    impl PermitCheckingWaker {
        fn record(&self) {
            self.permit_was_available
                .store(self.semaphore.available_permits() == 1, Ordering::SeqCst);
            self.called.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn total_connection_permit_releases_capacity_before_notifying() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).try_acquire_owned().unwrap();
        let connection_available = Arc::new(Notify::new());
        let mut notified = Box::pin(connection_available.notified());
        let checker = Arc::new(PermitCheckingWaker {
            semaphore,
            called: AtomicBool::new(false),
            permit_was_available: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&checker));
        let mut context = Context::from_waker(&waker);
        assert_eq!(notified.as_mut().poll(&mut context), Poll::Pending);

        drop(TotalConnectionPermit {
            permit: Some(permit),
            connection_available: Arc::clone(&connection_available),
        });

        assert!(checker.called.load(Ordering::SeqCst));
        assert!(checker.permit_was_available.load(Ordering::SeqCst));
    }

    #[test]
    fn test_datanode_url_uses_ip_address_by_default() {
        let datanode_id = hdfs::DatanodeIdProto {
            ip_addr: "127.0.0.1".to_string(),
            host_name: "hdfs-datanode".to_string(),
            xfer_port: 9866,
            ..Default::default()
        };
        let config =
            Configuration::new(Some("/tmp/hdfs-native-missing-conf".to_string()), None).unwrap();

        assert_eq!(datanode_url(&datanode_id, &config), "127.0.0.1:9866");
    }

    #[test]
    fn test_datanode_url_uses_hostname_when_configured() {
        let datanode_id = hdfs::DatanodeIdProto {
            ip_addr: "127.0.0.1".to_string(),
            host_name: "hdfs-datanode".to_string(),
            xfer_port: 9866,
            ..Default::default()
        };
        let config = Configuration::new(
            Some("/tmp/hdfs-native-missing-conf".to_string()),
            Some(
                [(
                    "dfs.client.use.datanode.hostname".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        )
        .unwrap();

        assert_eq!(datanode_url(&datanode_id, &config), "hdfs-datanode:9866");
    }

    fn encode_router_state(map: &HashMap<String, i64>) -> Vec<u8> {
        hdfs::RouterFederatedStateProto {
            namespace_state_ids: map.clone(),
        }
        .encode_to_vec()
    }

    #[test]
    fn test_router_federated_state() {
        let mut alignment_context = AlignmentContext::default();

        assert!(alignment_context.router_federated_state.is_none());

        let mut state_map = HashMap::<String, i64>::new();
        state_map.insert("ns-1".to_string(), 3);

        alignment_context
            .update(None, Some(encode_router_state(&state_map)))
            .unwrap();

        assert!(alignment_context.router_federated_state.is_some());

        let router_state = alignment_context.router_federated_state.as_ref().unwrap();

        assert_eq!(router_state.len(), 1);
        assert_eq!(*router_state.get("ns-1").unwrap(), 3);

        state_map.insert("ns-1".to_string(), 5);
        state_map.insert("ns-2".to_string(), 7);

        alignment_context
            .update(None, Some(encode_router_state(&state_map)))
            .unwrap();

        let router_state = alignment_context.router_federated_state.as_ref().unwrap();

        assert_eq!(router_state.len(), 2);
        assert_eq!(*router_state.get("ns-1").unwrap(), 5);
        assert_eq!(*router_state.get("ns-2").unwrap(), 7);
    }

    #[test]
    fn test_connection_context_uses_provided_user_info() {
        let (sender, _receiver) = mpsc::channel(1);
        let conn = RpcConnection {
            client_id: Vec::new(),
            user_info: UserInfo {
                real_user: Some("real-user".to_string()),
                effective_user: Some("alice".to_string()),
            },
            next_call_id: AtomicI32::new(0),
            alignment_context: None,
            call_map: Arc::new(Mutex::new(Some(HashMap::new()))),
            sender,
            listener: None,
        };

        let context = conn.get_connection_context();
        let user_info = context.user_info.unwrap();
        assert_eq!(user_info.real_user.as_deref(), Some("real-user"));
        assert_eq!(user_info.effective_user.as_deref(), Some("alice"));
    }

    fn read_packet(data: &'static [u8], checksums: &[u32]) -> ReadPacket {
        let mut checksum_bytes = BytesMut::new();
        for checksum in checksums {
            checksum_bytes.put_u32(*checksum);
        }
        ReadPacket::new(
            hdfs::PacketHeaderProto::default(),
            checksum_bytes.freeze(),
            Bytes::from_static(data),
        )
    }

    fn checksum_info(
        checksum_type: hdfs::ChecksumTypeProto,
        bytes_per_checksum: u32,
    ) -> Option<hdfs::ReadOpChecksumInfoProto> {
        Some(hdfs::ReadOpChecksumInfoProto {
            checksum: hdfs::ChecksumProto {
                r#type: checksum_type as i32,
                bytes_per_checksum,
            },
            chunk_offset: 0,
        })
    }

    #[test]
    fn test_crc32_verification() {
        // 0xCBF43926 is the java.util.zip.CRC32 value Hadoop stores for "123456789"
        let info = checksum_info(hdfs::ChecksumTypeProto::ChecksumCrc32, 512);
        let data = read_packet(b"123456789", &[0xCBF43926])
            .get_data(&info)
            .unwrap();
        assert_eq!(data, Bytes::from_static(b"123456789"));
    }

    #[test]
    fn test_crc32c_verification() {
        let info = checksum_info(hdfs::ChecksumTypeProto::ChecksumCrc32c, 512);
        let data = read_packet(b"123456789", &[0xE3069283])
            .get_data(&info)
            .unwrap();
        assert_eq!(data, Bytes::from_static(b"123456789"));
    }

    #[test]
    fn test_crc32c_software_fallback() {
        assert_eq!(CRC32C.checksum(b"123456789"), 0xE3069283);
    }

    #[test]
    fn test_crc32_multiple_chunks() {
        let info = checksum_info(hdfs::ChecksumTypeProto::ChecksumCrc32, 4);
        let checksums = [
            CRC32.checksum(b"1234"),
            CRC32.checksum(b"5678"),
            CRC32.checksum(b"9"),
        ];
        let data = read_packet(b"123456789", &checksums)
            .get_data(&info)
            .unwrap();
        assert_eq!(data, Bytes::from_static(b"123456789"));
    }

    #[test]
    fn test_checksum_mismatch() {
        let info = checksum_info(hdfs::ChecksumTypeProto::ChecksumCrc32c, 512);
        let err = read_packet(b"123456789", &[0xE3069284])
            .get_data(&info)
            .unwrap_err();
        assert!(matches!(err, HdfsError::ChecksumError));
    }

    #[test]
    fn test_crc32c_multiple_chunks_and_partial_tail() {
        let info = checksum_info(hdfs::ChecksumTypeProto::ChecksumCrc32c, 4);
        let checksums = [
            crc32c::crc32c(b"1234"),
            crc32c::crc32c(b"5678"),
            crc32c::crc32c(b"9"),
        ];
        let data = read_packet(b"123456789", &checksums)
            .get_data(&info)
            .unwrap();
        assert_eq!(data, Bytes::from_static(b"123456789"));
    }

    #[test]
    fn test_invalid_packet_lengths() {
        assert!(matches!(
            packet_body_length(3),
            Err(HdfsError::DataTransferError(_))
        ));
        assert!(matches!(
            packet_checksum_length(8, -1),
            Err(HdfsError::DataTransferError(_))
        ));
        assert!(matches!(
            packet_checksum_length(8, 9),
            Err(HdfsError::DataTransferError(_))
        ));
    }

    #[test]
    fn test_invalid_checksum_metadata() {
        let missing_checksum = checksum_info(hdfs::ChecksumTypeProto::ChecksumCrc32c, 4);
        let err = read_packet(b"123456789", &[crc32c::crc32c(b"1234")])
            .get_data(&missing_checksum)
            .unwrap_err();
        assert!(matches!(err, HdfsError::ChecksumError));

        let zero_chunk_size = checksum_info(hdfs::ChecksumTypeProto::ChecksumCrc32c, 0);
        let err = read_packet(b"123456789", &[])
            .get_data(&zero_chunk_size)
            .unwrap_err();
        assert!(matches!(err, HdfsError::ChecksumError));
    }

    #[test]
    fn test_write_packet_crc32c_checksums() {
        let mut packet = WritePacket::empty(0, 0, 4, 128);
        packet.data.extend_from_slice(b"123456789");

        let checksums = packet.calculate_checksum();
        let expected = [
            crc32c::crc32c(b"1234"),
            crc32c::crc32c(b"5678"),
            crc32c::crc32c(b"9"),
        ];
        let actual = checksums
            .chunks_exact(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
