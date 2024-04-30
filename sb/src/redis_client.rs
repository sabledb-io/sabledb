use crate::{
    response_validators::{
        validate_bulk_str, validate_null_string_or_number, validate_number, validate_status_ok,
        validate_status_pong,
    },
    stats,
};

use bytes::BytesMut;
use libsabledb::{
    BytesMutUtils, ParseResult, RedisObject, RespBuilderV2, RespResponseParserV2, SableError,
    StopWatch,
};
use pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::Error as TLSError;
use tokio_rustls::rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    DigitallySignedStruct,
};

use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
struct NoVerifier;

/// Allow this client to accept self signed certificates by installing a `NoVerifier`
impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        let schemes = vec![
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA1,
            tokio_rustls::rustls::SignatureScheme::ECDSA_SHA1_Legacy,
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA256,
            tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA384,
            tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA512,
            tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA256,
            tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA384,
            tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA512,
            tokio_rustls::rustls::SignatureScheme::ED25519,
            tokio_rustls::rustls::SignatureScheme::ED448,
        ];
        schemes
    }
}

#[derive(Default)]
pub struct SBClient {
    builder: RespBuilderV2,
}

async fn read_more_bytes(
    stream: &mut (impl AsyncReadExt + std::marker::Unpin),
    read_buffer: &mut BytesMut,
) -> Result<(), SableError> {
    let mut buffer = BytesMut::with_capacity(512);
    stream.read_buf(&mut buffer).await?;

    if buffer.is_empty() {
        return Err(SableError::OtherError(
            "Server closed connection".to_string(),
        ));
    }
    read_buffer.extend_from_slice(&buffer);
    Ok(())
}

async fn read_response(
    stream: &mut (impl AsyncReadExt + std::marker::Unpin),
    read_buffer: &mut BytesMut,
) -> Result<RedisObject, SableError> {
    loop {
        match RespResponseParserV2::parse_response(read_buffer)? {
            ParseResult::NeedMoreData => read_more_bytes(stream, read_buffer).await?,
            ParseResult::Ok((consume, obj)) => {
                let _ = read_buffer.split_to(consume);
                return Ok(obj);
            }
        }
    }
}

/// Pointer to a function for validating the received response
pub type ValidationFunction = fn(RedisObject) -> bool;

impl SBClient {
    /// Connect with retries
    async fn connect_with_retries(host: &String, port: u16) -> Result<TcpStream, SableError> {
        let connection_string = format!("{}:{}", host, port);
        let socket: SocketAddrV4 = connection_string.parse().expect("parse");
        let mut counter = 0u64;
        loop {
            let res = TcpStream::connect(socket).await;
            if let Ok(conn) = res {
                return Ok(conn);
            } else {
                counter = counter.saturating_add(1);
                tokio::time::sleep(tokio::time::Duration::from_millis(counter)).await;
                if counter == 100 {
                    return Err(SableError::OtherError(format!(
                        "Failed to connect. {:?}",
                        res.err()
                    )));
                }
            }
        }
    }

    pub async fn writer_loop(
        mut rx_channel: Receiver<(BytesMut, ValidationFunction)>,
        mut tx: impl AsyncWriteExt + std::marker::Unpin,
        tx_channel: Sender<(ValidationFunction, StopWatch)>,
    ) {
        while let Some((mut msg, validation_func)) = rx_channel.recv().await {
            if let Err(e) = tx.write_buf(&mut msg).await {
                tracing::error!("failed to send message over the socket. {:?}", e);
                break;
            }

            // Notify
            let sw = StopWatch::default();
            if let Err(e) = tx_channel.send((validation_func, sw)).await {
                tracing::error!("failed to send message to receiver task. {:?}", e);
                break;
            }
        }
    }

    pub async fn reader_loop(
        mut rx: impl AsyncReadExt + std::marker::Unpin,
        mut rx_channel: Receiver<(ValidationFunction, StopWatch)>,
    ) {
        // response reader task
        let mut buffer = BytesMut::new();
        loop {
            tokio::select! {
                response = read_response(&mut rx, &mut buffer) => {
                    let Ok(response) = response else {
                        tracing::error!("failed to read response from socket");
                        return;
                    };
                    stats::incr_requests();

                    // pull 1 entry from the channel
                    let Some((validation_func, sw)) = rx_channel.recv().await else {
                        tracing::error!("sender closed channel");
                        return;
                    };

                    stats::record_latency(
                        sw.elapsed_micros()
                            .unwrap()
                            .try_into()
                            .unwrap_or(u64::MAX)
                    );

                    if !validation_func(response) {
                        return;
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                    if stats::is_test_done() {
                        return;
                    }
                }
            }
        }
    }

    pub async fn connect(
        host: String,
        port: u16,
        ssl: bool,
        pipeline_size: usize,
    ) -> Result<Sender<(BytesMut, ValidationFunction)>, SableError> {
        let stream = Self::connect_with_retries(&host, port).await?;
        // create a channel to communicate between the sender and the receiver tasks
        let (channel_tx, channel_rx) =
            tokio::sync::mpsc::channel::<(ValidationFunction, StopWatch)>(pipeline_size);
        let (bytes_tx, bytes_rx) =
            tokio::sync::mpsc::channel::<(BytesMut, ValidationFunction)>(pipeline_size);
        if ssl {
            let mut root_cert_store = rustls::RootCertStore::empty();
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = tokio_rustls::rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(
                    crate::redis_client::NoVerifier {},
                ))
                .with_no_client_auth(); // i guess this was previously the default?
            let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
            let dns: ServerName = host.try_into().expect("invalid DNS name");
            let stream = connector.connect(dns, stream).await?;
            let (rx, tx) = tokio::io::split(stream);

            // start 2 tasks:
            // - reader: reads resonse from `rx` (network reader end) and compare the result to the front entry in the
            //   `channel_rx`
            // - writer: accepts raw RESP message on the `bytes_rx` channel and send it over the network to the SableDb
            //    using `tx`
            tokio::task::spawn_local(async move {
                let _ = Self::reader_loop(rx, channel_rx).await;
            });

            tokio::task::spawn_local(async move {
                let _ = Self::writer_loop(bytes_rx, tx, channel_tx).await;
            });
        } else {
            let (rx, tx) = tokio::io::split(stream);
            tokio::task::spawn_local(async move {
                let _ = Self::reader_loop(rx, channel_rx).await;
            });

            tokio::task::spawn_local(async move {
                let _ = Self::writer_loop(bytes_rx, tx, channel_tx).await;
            });
        };
        Ok(bytes_tx)
    }

    pub async fn set(
        &mut self,
        tx: &mut Sender<(BytesMut, ValidationFunction)>,
        key: &BytesMut,
        value: &BytesMut,
    ) -> Result<(), SableError> {
        // prepare and send command
        let mut buffer = BytesMut::new();
        self.builder.add_array_len(&mut buffer, 3);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("set"));
        self.builder.add_bulk_string(&mut buffer, key);
        self.builder.add_bulk_string(&mut buffer, value);

        if let Err(e) = tx.send((buffer, validate_status_ok)).await {
            return Err(SableError::OtherError(format!(
                "Failed to send message to writer. {:?}",
                e
            )));
        }
        Ok(())
    }

    pub async fn hset(
        &mut self,
        tx: &mut Sender<(BytesMut, ValidationFunction)>,
        key: &BytesMut,
        field: &BytesMut,
        value: &BytesMut,
    ) -> Result<(), SableError> {
        let mut buffer = BytesMut::new();
        // build the command
        self.builder.add_array_len(&mut buffer, 4);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("hset"));
        self.builder.add_bulk_string(&mut buffer, key);
        self.builder.add_bulk_string(&mut buffer, field);
        self.builder.add_bulk_string(&mut buffer, value);

        if let Err(e) = tx.send((buffer, validate_null_string_or_number)).await {
            return Err(SableError::OtherError(format!(
                "Failed to send message to writer. {:?}",
                e
            )));
        }
        Ok(())
    }

    pub async fn get(
        &mut self,
        tx: &mut Sender<(BytesMut, ValidationFunction)>,
        key: &BytesMut,
    ) -> Result<(), SableError> {
        // prepare and send command
        let mut buffer = BytesMut::new();
        self.builder.add_array_len(&mut buffer, 2);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("get"));
        self.builder.add_bulk_string(&mut buffer, key);
        if let Err(e) = tx.send((buffer, validate_bulk_str)).await {
            return Err(SableError::OtherError(format!(
                "Failed to send message to writer. {:?}",
                e
            )));
        }
        Ok(())
    }

    pub async fn ping(
        &mut self,
        tx: &mut Sender<(BytesMut, ValidationFunction)>,
    ) -> Result<(), SableError> {
        // prepare and send command
        let mut buffer = BytesMut::new();
        self.builder.add_array_len(&mut buffer, 1);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("ping"));
        if let Err(e) = tx.send((buffer, validate_status_pong)).await {
            return Err(SableError::OtherError(format!(
                "Failed to send message to writer. {:?}",
                e
            )));
        }
        Ok(())
    }

    pub async fn incr(
        &mut self,
        tx: &mut Sender<(BytesMut, ValidationFunction)>,
        key: &BytesMut,
        incremenet: u64,
    ) -> Result<(), SableError> {
        let mut buffer = BytesMut::new();
        self.builder.add_array_len(&mut buffer, 3);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("incrby"));
        self.builder.add_bulk_string(&mut buffer, key);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMutUtils::from::<u64>(&incremenet));
        if let Err(e) = tx.send((buffer, validate_number)).await {
            return Err(SableError::OtherError(format!(
                "Failed to send message to writer. {:?}",
                e
            )));
        }
        Ok(())
    }

    pub async fn push(
        &mut self,
        tx: &mut Sender<(BytesMut, ValidationFunction)>,
        key: &BytesMut,
        value: &BytesMut,
        right: bool,
    ) -> Result<(), SableError> {
        let mut buffer = BytesMut::new();
        let cmd = if right {
            BytesMut::from("rpush")
        } else {
            BytesMut::from("lpush")
        };
        self.builder.add_array_len(&mut buffer, 3);
        self.builder.add_bulk_string(&mut buffer, &cmd);
        self.builder.add_bulk_string(&mut buffer, key);
        self.builder.add_bulk_string(&mut buffer, value);
        if let Err(e) = tx.send((buffer, validate_number)).await {
            return Err(SableError::OtherError(format!(
                "Failed to send message to writer. {:?}",
                e
            )));
        }
        Ok(())
    }

    pub async fn pop(
        &mut self,
        tx: &mut Sender<(BytesMut, ValidationFunction)>,
        key: &BytesMut,
        right: bool,
    ) -> Result<(), SableError> {
        let mut buffer = BytesMut::new();
        let cmd = if right {
            BytesMut::from("rpop")
        } else {
            BytesMut::from("lpop")
        };
        self.builder.add_array_len(&mut buffer, 2);
        self.builder.add_bulk_string(&mut buffer, &cmd);
        self.builder.add_bulk_string(&mut buffer, key);
        if let Err(e) = tx.send((buffer, validate_bulk_str)).await {
            return Err(SableError::OtherError(format!(
                "Failed to send message to writer. {:?}",
                e
            )));
        }
        Ok(())
    }
}
