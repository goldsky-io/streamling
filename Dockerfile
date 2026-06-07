FROM gcr.io/distroless/cc:latest

# Copy prebuilt Linux binary
# Default config and the WASM runtime are embedded in the binary, so no files are
# copied here. To override config, mount a config.yaml into the working directory
# below, or set STREAMLING__* environment variables.
COPY target/release/streamling /usr/local/bin/streamling

# Set working directory
WORKDIR /opt/streamling

# Use non-root user (distroless has nonroot user = 65532)
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/streamling"]
