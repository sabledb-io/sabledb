use bytes::BytesMut;
use libsabledb::{
    BytesMutUtils, ParseResult, RedisObject, RespBuilderV2, RespResponseParserV2, SableError,
};
use pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::Error as TLSError;
use tokio_rustls::rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    DigitallySignedStruct,
};

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

pub enum StreamType {
    Tls(TlsStream<TcpStream>),
    Plain(TcpStream),
}

#[derive(Default)]
pub struct RedisClient {
    builder: RespBuilderV2,
    read_buffer: BytesMut,
}

impl RedisClient {
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
                counter += 1;
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

    pub async fn connect(host: String, port: u16, ssl: bool) -> Result<StreamType, SableError> {
        let stream = Self::connect_with_retries(&host, port).await?;
        let stream = if ssl {
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

            StreamType::Tls(stream)
        } else {
            StreamType::Plain(stream)
        };
        Ok(stream)
    }

    pub async fn write_buffer(
        &mut self,
        stream: &mut StreamType,
        buffer: &BytesMut,
    ) -> Result<(), SableError> {
        match stream {
            StreamType::Tls(s) => {
                s.write_all(buffer).await?;
            }
            StreamType::Plain(s) => {
                s.write_all(buffer).await?;
            }
        }
        Ok(())
    }

    pub async fn set(
        &mut self,
        stream: &mut StreamType,
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

        self.write_buffer(stream, &buffer).await?;

        // read response
        let RedisObject::Status(msg) = self.read_response(stream).await? else {
            return Err(SableError::OtherError("expected 'OK'".to_string()));
        };

        if !msg.eq("OK") {
            return Err(SableError::OtherError("expected 'OK'".to_string()));
        }
        Ok(())
    }

    pub async fn hset(
        &mut self,
        stream: &mut StreamType,
        key: &BytesMut,
        field: &BytesMut,
        value: &BytesMut,
    ) -> Result<RedisObject, SableError> {
        let mut buffer = BytesMut::new();
        // build the command
        self.builder.add_array_len(&mut buffer, 4);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("hset"));
        self.builder.add_bulk_string(&mut buffer, key);
        self.builder.add_bulk_string(&mut buffer, field);
        self.builder.add_bulk_string(&mut buffer, value);

        // send the request & read the response
        self.write_buffer(stream, &buffer).await?;

        // read response
        match self.read_response(stream).await? {
            RedisObject::NullString => Ok(RedisObject::NullString),
            RedisObject::Integer(num) => Ok(RedisObject::Integer(num)),
            other => Err(SableError::OtherError(format!(
                "Unexpected response. `{:?}`",
                other
            ))),
        }
    }

    pub async fn get(
        &mut self,
        stream: &mut StreamType,
        key: &BytesMut,
    ) -> Result<RedisObject, SableError> {
        // prepare and send command
        let mut buffer = BytesMut::new();
        self.builder.add_array_len(&mut buffer, 2);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("get"));
        self.builder.add_bulk_string(&mut buffer, key);
        self.write_buffer(stream, &buffer).await?;

        // read response
        match self.read_response(stream).await? {
            RedisObject::Str(value) => Ok(RedisObject::Str(value)),
            RedisObject::NullString => Ok(RedisObject::NullString),
            _ => Err(SableError::OtherError("expected String object".to_string())),
        }
    }

    pub async fn ping(&mut self, stream: &mut StreamType) -> Result<(), SableError> {
        // prepare and send command
        let mut buffer = BytesMut::new();
        self.builder.add_array_len(&mut buffer, 1);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("ping"));
        self.write_buffer(stream, &buffer).await?;

        // read response
        let RedisObject::Status(pong) = self.read_response(stream).await? else {
            return Err(SableError::OtherError("expected Status object".to_string()));
        };

        if !pong.eq("PONG") {
            return Err(SableError::OtherError("expected 'PONG'".to_string()));
        }
        Ok(())
    }

    pub async fn incr(
        &mut self,
        stream: &mut StreamType,
        key: &BytesMut,
        incremenet: u64,
    ) -> Result<u64, SableError> {
        let mut buffer = BytesMut::new();
        self.builder.add_array_len(&mut buffer, 3);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMut::from("incrby"));
        self.builder.add_bulk_string(&mut buffer, key);
        self.builder
            .add_bulk_string(&mut buffer, &BytesMutUtils::from::<u64>(&incremenet));
        self.write_buffer(stream, &buffer).await?;

        // read response
        match self.read_response(stream).await? {
            RedisObject::Integer(val) => Ok(val),
            other => Err(SableError::OtherError(format!(
                "Expected Integer object. Received {:?}",
                other
            ))),
        }
    }

    pub async fn push(
        &mut self,
        stream: &mut StreamType,
        key: &BytesMut,
        value: &BytesMut,
        right: bool,
    ) -> Result<u64, SableError> {
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
        self.write_buffer(stream, &buffer).await?;

        // read response
        match self.read_response(stream).await? {
            RedisObject::Integer(list_length) => Ok(list_length),
            other => Err(SableError::OtherError(format!(
                "Expected Integer object. Received {:?}",
                other
            ))),
        }
    }

    pub async fn pop(
        &mut self,
        stream: &mut StreamType,
        key: &BytesMut,
        right: bool,
    ) -> Result<RedisObject, SableError> {
        let mut buffer = BytesMut::new();
        let cmd = if right {
            BytesMut::from("rpop")
        } else {
            BytesMut::from("lpop")
        };
        self.builder.add_array_len(&mut buffer, 2);
        self.builder.add_bulk_string(&mut buffer, &cmd);
        self.builder.add_bulk_string(&mut buffer, key);
        self.write_buffer(stream, &buffer).await?;

        // read response
        match self.read_response(stream).await? {
            RedisObject::NullString => Ok(RedisObject::NullString),
            RedisObject::Str(s) => Ok(RedisObject::Str(s)),
            other => Err(SableError::OtherError(format!(
                "Unexpected response. `{:?}`",
                other
            ))),
        }
    }

    async fn read_more_bytes(&mut self, stream: &mut StreamType) -> Result<(), SableError> {
        let mut buffer = BytesMut::with_capacity(512);
        match stream {
            StreamType::Tls(s) => {
                s.read_buf(&mut buffer).await?;
            }
            StreamType::Plain(s) => {
                s.read_buf(&mut buffer).await?;
            }
        }

        if buffer.is_empty() {
            return Err(SableError::OtherError(
                "Server closed connection".to_string(),
            ));
        }
        self.read_buffer.extend_from_slice(&buffer);
        Ok(())
    }

    async fn read_response(&mut self, stream: &mut StreamType) -> Result<RedisObject, SableError> {
        loop {
            match RespResponseParserV2::parse_response(&self.read_buffer)? {
                ParseResult::NeedMoreData => self.read_more_bytes(stream).await?,
                ParseResult::Ok((consume, obj)) => {
                    let _ = self.read_buffer.split_to(consume);
                    return Ok(obj);
                }
            }
        }
    }
}
