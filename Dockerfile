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
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder /app/target/release/sabledb /usr/local/bin/sabledb

# Set the entrypoint for the Docker container
ENTRYPOINT ["sabledb"]

# Expose any required ports (update this according to your project's needs)
EXPOSE 6379
