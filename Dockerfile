# Use the official Rust image as the base image
FROM rust:1.82-slim AS builder

# Install system dependencies needed for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the Cargo files first for better Docker layer caching
COPY Cargo.toml Cargo.lock ./

# Pre-fetch dependencies for layer caching (does not build the binary)
# Create a temporary dummy target so Cargo sees a target in the manifest
RUN mkdir -p src \
    && echo "fn main() {}" > src/main.rs \
    && cargo fetch --locked \
    && rm -rf src

# Now copy the actual source code
COPY src/ ./src/

# Build the actual application
RUN cargo build --release --locked

# Use a minimal runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/subgraph-converter /app/subgraph-converter

# Expose the port the app runs on
EXPOSE 3000

# Set environment variables with defaults
ENV RUST_LOG=info

# Run the application
CMD ["./subgraph-converter"]
