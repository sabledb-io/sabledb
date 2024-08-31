# Use the Rust image based on Debian Bookworm as the base image
FROM rust:slim-bookworm as builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    clang \
    git \
    && rm -rf /var/lib/apt/lists/*

# Clone the repository
WORKDIR /app
RUN git clone https://github.com/sabledb-io/sabledb.git .
WORKDIR /app

# Build the project
RUN cargo build --release

# Copy the build artifact to a slim image
FROM debian:bookworm-slim

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libclang-dev \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder             \
    /app/target/release/sabledb \
    /app/target/release/sb      \
    /app/target/release/sdb-cli \
    /usr/local/bin/

RUN mkdir -p              \
    /var/lib/sabledb/conf \
    /var/lib/sabledb/log  \
    /var/lib/sabledb/data \
    /etc/sabledb

# Generate INI file
RUN echo '\
[general]\n\
logdir = "/var/lib/sabledb/log"\n\
public_address = "0.0.0.0:6379"\n\
db_path = "/var/lib/sabledb/data/sabledb.db"\n\
config_dir = "/var/lib/sabledb/conf"'\
>> /etc/sabledb/server.ini

# Set the entrypoint for the Docker container
ENTRYPOINT ["/usr/local/bin/sabledb", "/etc/sabledb/server.ini"]

# Expose any required ports (update this according to your project's needs)
EXPOSE 6379
