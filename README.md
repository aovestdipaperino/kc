# kc - Kafka CLI Tool

[![GitHub stars](https://img.shields.io/github/stars/aovestdipaperino/kc?logo=github&color=brightgreen)](https://github.com/aovestdipaperino/kc/stargazers)

A lightweight command-line tool for debugging and managing Kafka clusters using the native Kafka protocol.

## Features

- Direct Kafka protocol communication without heavyweight dependencies
- Automatic API version negotiation with broker compatibility detection
- Intelligent caching of API versions for optimal performance
- Support for topic management (create, delete, inspect)
- Message publishing and fetching capabilities
- Cluster metadata inspection
- Built with Rust for speed and reliability

## Installation

### Prerequisites

- Rust 1.70 or higher
- Cargo

### Build from Source

```bash
cargo build --release
```

The binary will be available at `target/release/kc`.

## Usage

### Basic Syntax

```bash
kc <BROKER> <COMMAND> [ARGUMENTS]
```

All commands require the broker address as the first argument.

### Commands

#### Get Metadata

Retrieve metadata information about the Kafka cluster, including brokers, topics, and partitions.

```bash
# Get metadata for all topics
kc localhost:9092 get-metadata

# Get metadata for a specific topic
kc localhost:9092 get-metadata --topic my-topic

# Connect to a remote broker
kc kafka.example.com:9092 get-metadata
```

#### Get Topic Information

Get detailed information about a specific topic.

```bash
kc localhost:9092 get-topic my-topic
```

Shows:
- Topic ID
- Partition count
- Partition details (leader, replicas, ISR)
- Internal topic flag

#### Create Topic

Create a new topic with an optional partition count.

```bash
# Create topic with 1 partition (default)
kc localhost:9092 create-topic my-topic

# Create topic with 3 partitions
kc localhost:9092 create-topic my-topic:3
```

Behavior:
- If topic exists with matching partitions: reports "Topic already exists"
- If topic exists with different partitions: asks for confirmation to recreate (y/n)
- Validates partition count before creation

#### Delete Topic

Delete an existing topic.

```bash
kc localhost:9092 delete-topic my-topic
```

Verifies the topic exists before attempting deletion.

#### Publish Message

Publish a message to a topic with a key and optional value.

```bash
# Publish to partition 0 (default)
kc localhost:9092 publish my-topic key1 value1

# Publish to specific partition
kc localhost:9092 publish my-topic:2 key2 value2

# Publish with key only (no value)
kc localhost:9092 publish my-topic key3
```

Features:
- Validates topic and partition exist before publishing
- Shows partition, offset, and timestamp on success
- Clear error messages with error codes

#### Fetch Messages

Fetch and display messages from a topic partition.

```bash
# Fetch from partition 0, starting at offset 0
kc localhost:9092 fetch my-topic:0

# Fetch from specific offset
kc localhost:9092 fetch my-topic:0:100

# Fetch from partition 2, offset 50
kc localhost:9092 fetch my-topic:2:50
```

Features:
- Validates topic and partition exist before fetching
- Displays key-value pairs for each message
- Default offset is 0 (beginning)

## API Version Negotiation

The client automatically handles API version negotiation:

1. On the first request, it fetches and caches all supported API versions from the broker
2. For each subsequent API call, it negotiates the best compatible version
3. The negotiated version is used for encoding requests and decoding responses

This ensures compatibility across different Kafka broker versions without hardcoding version numbers.

## Examples

### Complete Workflow

```bash
# Set broker for convenience
BROKER=localhost:9092

# Check cluster metadata
kc $BROKER get-metadata

# Create a topic with 3 partitions
kc $BROKER create-topic orders:3

# Verify topic was created
kc $BROKER get-topic orders

# Publish some messages
kc $BROKER publish orders:0 order-1 '{"item":"laptop","price":999}'
kc $BROKER publish orders:1 order-2 '{"item":"mouse","price":25}'
kc $BROKER publish orders:2 order-3 '{"item":"keyboard","price":75}'

# Fetch messages
kc $BROKER fetch orders:0:0
kc $BROKER fetch orders:1:0
kc $BROKER fetch orders:2:0

# Delete the topic
kc $BROKER delete-topic orders
```

### Debug Mode

Enable detailed logging with the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug kc localhost:9092 create-topic my-topic:3
```

This will show:
- API version negotiation details
- Request/response sizes
- Wire protocol information
- Record batch encoding details
- Full error responses

## Architecture

### KafkaClient

The core client implementation includes:

- **Connection Management**: TCP socket connection to Kafka brokers
- **API Version Cache**: Automatic caching of broker-supported API versions
- **Version Negotiation**: Intelligent selection of compatible API versions
- **Protocol Handling**: Request encoding and response decoding using `kafka-protocol` crate

### Request Flow

```
1. Client connects to broker
2. On first API call:
   - Fetch API versions from broker
   - Cache version information
3. For each request:
   - Negotiate compatible API version
   - Encode request with negotiated version
   - Send request over TCP
   - Receive and decode response
```

### Supported API Versions

For maximum compatibility, the tool uses conservative API versions:
- **CreateTopics**: Version 4
- **DeleteTopics**: Version 4
- **Produce**: Version 7
- **Fetch**: Version 11
- **Metadata**: Version 12

## Dependencies

- `tokio` - Async runtime for I/O operations
- `kafka-protocol` - Kafka protocol serialization/deserialization
- `anyhow` - Error handling
- `clap` - Command-line argument parsing
- `bytes` - Byte buffer utilities
- `tracing` - Structured logging

## Development

### Running Tests

The project includes comprehensive unit tests covering:
- Argument parsing and validation
- Kafka error codes
- Record batch encoding/decoding
- API version negotiation logic
- Topic name and partition validation
- Broker address parsing

Run all tests:

```bash
cargo test
```

Run tests with output:

```bash
cargo test -- --nocapture
```

Run specific test:

```bash
cargo test test_record_batch_encoding
```

### Test Coverage

Current test suite includes 16+ tests covering:
- **Parsing**: Topic specs, partition numbers, offsets, broker addresses
- **Validation**: Partition counts, topic names, offset ranges
- **Protocol**: Record encoding/decoding, error codes, API version negotiation
- **Logic**: Correlation ID management, bytes conversion

### Building for Release

```bash
cargo build --release
```

### Code Structure

- `src/main.rs` - Main entry point, KafkaClient implementation, and tests
- `Cargo.toml` - Project dependencies and metadata

## Protocol Details

This tool implements the Kafka wire protocol directly:

- **Frame Format**: `[4 bytes: size][request/response data]`
- **Request Structure**: `[RequestHeader][Request Body]`
- **Response Structure**: `[ResponseHeader][Response Body]`
- **API Version Support**: Dynamically queried from broker
- **Record Format**: Version 2 (record batches)

## Error Codes

Common Kafka error codes you might encounter:

- **0**: No error (success)
- **1**: OFFSET_OUT_OF_RANGE - Requested offset is invalid
- **3**: UNKNOWN_TOPIC_OR_PARTITION - Topic or partition doesn't exist
- **5**: LEADER_NOT_AVAILABLE - Partition leader is not available
- **6**: NOT_LEADER_FOR_PARTITION - Broker is not the leader
- **36**: TOPIC_ALREADY_EXISTS - Topic creation failed (already exists)
- **37**: INVALID_PARTITIONS - Invalid partition count
- **38**: INVALID_REPLICATION_FACTOR - Invalid replication factor

## Troubleshooting

### Connection Issues

If you encounter connection problems:

```bash
# Verify broker is accessible
telnet localhost 9092

# Check with debug logging
RUST_LOG=debug kc localhost:9092 get-metadata
```

### Topic Already Exists

When creating a topic that already exists:
- If partition count matches: Tool reports the topic exists
- If partition count differs: Tool asks if you want to recreate it
- Recreating deletes the old topic and creates a new one

### Publishing Not Working

If publish succeeds but fetch returns no messages:
1. Verify the topic and partition exist: `kc <broker> get-topic <topic>`
2. Check you're fetching from the correct partition
3. Run with `RUST_LOG=debug` to see detailed error information
4. Ensure the broker is properly configured and has sufficient resources

### API Version Errors

If you see API version errors:
1. Check that your Kafka broker is running and accessible
2. Verify the broker version supports the requested APIs
3. Enable debug logging to see version negotiation details

### Performance

The client caches API versions per connection, so:
- First request will make two calls (API versions + actual request)
- Subsequent requests only make the actual API call
- Each command creates a new connection (stateless)

## Contributing

Contributions are welcome! Please ensure:

1. Code compiles without warnings
2. All tests pass
3. New features include appropriate documentation
4. Follow Rust best practices and idioms

## License

Apache-2.0
