pub struct Transport {}

impl Transport {
    pub fn prepare_std_tcp_stream(stream: &std::net::TcpStream) {
        let _ = stream.set_nonblocking(true);
        let _ = stream.set_nodelay(true);
    }
}
