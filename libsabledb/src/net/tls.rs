use crate::SableError;
use pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    certs(&mut BufReader::new(File::open(path)?)).collect()
}

fn load_keys(path: &Path) -> io::Result<Option<PrivateKeyDer<'static>>> {
    private_key(&mut BufReader::new(File::open(path)?))
}

pub fn create_tls_acceptor(
    cert: &Path,
    key: &Path,
) -> Result<tokio_rustls::TlsAcceptor, SableError> {
    let cert = load_certs(cert)?;
    let Some(pk) = load_keys(key)? else {
        tracing::error!("failed to load TLS key");
        return Err(SableError::NotFound);
    };

    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, pk)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    #[test]
    fn test_loading_cert() {
        let cert = PathBuf::from("../ssl/sabledb.crt");
        assert!(load_certs(&cert).is_ok());
    }

    #[test]
    fn test_loading_key() {
        let key = PathBuf::from("../ssl/sabledb.key");
        assert!(load_keys(&key).is_ok());
    }
}
