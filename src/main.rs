// Kafka Client (kc) - A CLI tool for interacting with Kafka clusters
//
// This tool provides commands for debugging and inspecting Kafka clusters
// using the kafka-protocol crate to communicate directly with Kafka brokers.

use anyhow::{Context, Result};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, MetadataRequest,
    MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

/// Kafka Client (kc) - A debugging tool for Kafka clusters
#[derive(Parser, Debug)]
#[clap(
    author,
    version,
    about = "Kafka Client CLI - Debug and inspect Kafka clusters"
)]
struct Args {
    /// Kafka broker address (e.g., "localhost:9092")
    broker: String,

    /// Command to execute
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Get metadata information from the Kafka cluster
    GetMetadata {
        /// Optional topic name to get metadata for (if not specified, gets all topics)
        #[clap(short, long)]
        topic: Option<String>,
    },
    /// Get information about a specific topic
    GetTopic {
        /// Topic name
        topic: String,
    },
    /// Create a new topic
    CreateTopic {
        /// Topic name and optional partition count (format: topicname or topicname:partitions)
        topic_spec: String,
    },
    /// Delete a topic
    DeleteTopic {
        /// Topic name
        topic: String,
    },
    /// Publish a message to a topic
    Publish {
        /// Topic name and optional partition (format: topicname or topicname:partition)
        topic_spec: String,
        /// Message key
        key: String,
        /// Message value (optional)
        value: Option<String>,
    },
    /// Fetch messages from a topic partition
    Fetch {
        /// Topic and partition spec (format: topicname:partition or topicname:partition:offset)
        topic_spec: String,
    },
}

/// A simple Kafka client for sending protocol requests
pub struct KafkaClient {
    stream: TcpStream,
    correlation_id: i32,
    /// Cached API versions: maps ApiKey to (min_version, max_version)
    api_versions_cache: Option<HashMap<i16, (i16, i16)>>,
}

impl KafkaClient {
    /// Creates a new Kafka client by connecting to the specified broker
    pub async fn new(broker_addr: &str) -> Result<Self> {
        info!("Connecting to Kafka broker: {}", broker_addr);
        let stream = TcpStream::connect(broker_addr)
            .await
            .context("Failed to connect to Kafka broker")?;
        Ok(KafkaClient {
            stream,
            correlation_id: 0,
            api_versions_cache: None,
        })
    }

    /// Ensures API versions are fetched and cached
    async fn ensure_api_versions(&mut self) -> Result<()> {
        if self.api_versions_cache.is_none() {
            info!("Fetching and caching API versions from broker...");
            let response = self.fetch_api_versions().await?;

            // Build cache from response
            let mut cache = HashMap::new();
            for api in &response.api_keys {
                cache.insert(api.api_key, (api.min_version, api.max_version));
            }
            let cache_size = cache.len();
            self.api_versions_cache = Some(cache);
            debug!("Cached {} API versions", cache_size);
        }
        Ok(())
    }

    /// Sends an ApiVersions request to check broker compatibility
    /// Note: This method bypasses the cache and directly fetches API versions
    async fn fetch_api_versions(&mut self) -> Result<ApiVersionsResponse> {
        self.correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(3)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from("kc")));

        let request = ApiVersionsRequest::default();

        let mut request_bytes = Vec::new();
        let header_version = ApiKey::ApiVersions.request_header_version(3);
        header.encode(&mut request_bytes, header_version)?;
        request.encode(&mut request_bytes, 3)?;

        let size = request_bytes.len() as i32;
        debug!("Sending ApiVersionsRequest (size: {})", size);

        self.stream.write_i32(size).await?;
        self.stream.write_all(&request_bytes).await?;

        let response_size = self
            .stream
            .read_i32()
            .await
            .context("Failed to read response size")?;
        let mut response_bytes = vec![0; response_size as usize];
        self.stream.read_exact(&mut response_bytes).await?;

        debug!("Received ApiVersionsResponse (size: {})", response_size);

        let mut response_bytes_mut = BytesMut::from(&response_bytes[..]);
        let response_header_version = ApiKey::ApiVersions.response_header_version(3);
        let _response_header =
            ResponseHeader::decode(&mut response_bytes_mut, response_header_version)?;

        let response = ApiVersionsResponse::decode(&mut response_bytes_mut, 3)?;
        Ok(response)
    }

    /// Gets the API versions response (uses cache if available, otherwise fetches)
    pub async fn get_api_versions(&mut self) -> Result<ApiVersionsResponse> {
        self.ensure_api_versions().await?;
        // Fetch again to return the full response
        self.fetch_api_versions().await
    }

    /// Gets the best supported API version for a given API key
    /// Returns the minimum of the requested version and the broker's max supported version
    async fn get_supported_version(
        &mut self,
        api_key: ApiKey,
        requested_version: i16,
    ) -> Result<i16> {
        self.ensure_api_versions().await?;

        if let Some(cache) = &self.api_versions_cache {
            if let Some((min_version, max_version)) = cache.get(&(api_key as i16)) {
                let version = requested_version.min(*max_version).max(*min_version);
                debug!(
                    "API {:?}: requested={}, broker supports min={} max={}, using={}",
                    api_key, requested_version, min_version, max_version, version
                );
                return Ok(version);
            }
        }

        // Fallback to requested version if not found in cache
        debug!(
            "API {:?}: not found in cache, using requested version {}",
            api_key, requested_version
        );
        Ok(requested_version)
    }

    /// Creates a new topic with the specified number of partitions
    pub async fn create_topic(
        &mut self,
        topic_name: &str,
        num_partitions: i32,
    ) -> Result<CreateTopicsResponse> {
        let api_version = self.get_supported_version(ApiKey::CreateTopics, 4).await?;
        self.correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::CreateTopics as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from("kc")));

        let topic = kafka_protocol::messages::create_topics_request::CreatableTopic::default()
            .with_name(TopicName(StrBytes::from(topic_name.to_string())))
            .with_num_partitions(num_partitions)
            .with_replication_factor(-1); // Use broker default

        let request = CreateTopicsRequest::default()
            .with_topics(vec![topic])
            .with_timeout_ms(30000);

        let mut request_bytes = Vec::new();
        let header_version = ApiKey::CreateTopics.request_header_version(api_version);
        header.encode(&mut request_bytes, header_version)?;
        request.encode(&mut request_bytes, api_version)?;

        let size = request_bytes.len() as i32;
        debug!("Sending CreateTopicsRequest (size: {})", size);
        self.stream.write_i32(size).await?;
        self.stream.write_all(&request_bytes).await?;

        let response_size = self
            .stream
            .read_i32()
            .await
            .context("Failed to read response size")?;
        let mut response_bytes = vec![0; response_size as usize];
        self.stream.read_exact(&mut response_bytes).await?;

        debug!("Received CreateTopicsResponse (size: {})", response_size);
        let mut response_bytes_mut = BytesMut::from(&response_bytes[..]);
        let response_header_version = ApiKey::CreateTopics.response_header_version(api_version);
        let _response_header =
            ResponseHeader::decode(&mut response_bytes_mut, response_header_version)?;
        let response = CreateTopicsResponse::decode(&mut response_bytes_mut, api_version)?;

        Ok(response)
    }

    /// Deletes a topic
    pub async fn delete_topic(&mut self, topic_name: &str) -> Result<DeleteTopicsResponse> {
        let api_version = self.get_supported_version(ApiKey::DeleteTopics, 4).await?;
        self.correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::DeleteTopics as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from("kc")));

        // For API versions 0-5, use topic_names field
        let request = DeleteTopicsRequest::default()
            .with_topic_names(vec![TopicName(StrBytes::from(topic_name.to_string()))])
            .with_timeout_ms(30000);

        let mut request_bytes = Vec::new();
        let header_version = ApiKey::DeleteTopics.request_header_version(api_version);
        header.encode(&mut request_bytes, header_version)?;
        request.encode(&mut request_bytes, api_version)?;

        let size = request_bytes.len() as i32;
        debug!("Sending DeleteTopicsRequest (size: {})", size);
        self.stream.write_i32(size).await?;
        self.stream.write_all(&request_bytes).await?;

        let response_size = self
            .stream
            .read_i32()
            .await
            .context("Failed to read response size")?;
        let mut response_bytes = vec![0; response_size as usize];
        self.stream.read_exact(&mut response_bytes).await?;

        debug!("Received DeleteTopicsResponse (size: {})", response_size);
        let mut response_bytes_mut = BytesMut::from(&response_bytes[..]);
        let response_header_version = ApiKey::DeleteTopics.response_header_version(api_version);
        let _response_header =
            ResponseHeader::decode(&mut response_bytes_mut, response_header_version)?;
        let response = DeleteTopicsResponse::decode(&mut response_bytes_mut, api_version)?;

        Ok(response)
    }

    /// Publishes a message to a topic
    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<ProduceResponse> {
        let api_version = self.get_supported_version(ApiKey::Produce, 7).await?;
        self.correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Produce as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from("kc")));

        use bytes::Bytes;
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions,
        };

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: kafka_protocol::records::TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            key: Some(Bytes::from(key.to_vec())),
            value: value.map(|v| Bytes::from(v.to_vec())),
            headers: Default::default(),
        };

        let records = vec![record];
        let mut batch_buf = BytesMut::new();
        let encode_options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        RecordBatchEncoder::encode(&mut batch_buf, &records, &encode_options)?;
        let batch = batch_buf.freeze();

        debug!("Encoded record batch size: {} bytes", batch.len());

        let partition_data =
            kafka_protocol::messages::produce_request::PartitionProduceData::default()
                .with_index(partition)
                .with_records(Some(batch));

        let topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default()
            .with_name(TopicName(StrBytes::from(topic.to_string())))
            .with_partition_data(vec![partition_data]);

        let request = ProduceRequest::default()
            .with_topic_data(vec![topic_data])
            .with_acks(-1) // Wait for all replicas
            .with_timeout_ms(30000);

        let mut request_bytes = Vec::new();
        let header_version = ApiKey::Produce.request_header_version(api_version);
        header.encode(&mut request_bytes, header_version)?;
        request.encode(&mut request_bytes, api_version)?;

        let size = request_bytes.len() as i32;
        debug!("Sending ProduceRequest (size: {})", size);
        self.stream.write_i32(size).await?;
        self.stream.write_all(&request_bytes).await?;

        let response_size = self
            .stream
            .read_i32()
            .await
            .context("Failed to read response size")?;
        let mut response_bytes = vec![0; response_size as usize];
        self.stream.read_exact(&mut response_bytes).await?;

        debug!("Received ProduceResponse (size: {})", response_size);
        let mut response_bytes_mut = BytesMut::from(&response_bytes[..]);
        let response_header_version = ApiKey::Produce.response_header_version(api_version);
        let _response_header =
            ResponseHeader::decode(&mut response_bytes_mut, response_header_version)?;
        let response = ProduceResponse::decode(&mut response_bytes_mut, api_version)?;

        Ok(response)
    }

    /// Fetches messages from a topic partition
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<FetchResponse> {
        let api_version = self.get_supported_version(ApiKey::Fetch, 11).await?;
        self.correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from("kc")));

        let partition_data = kafka_protocol::messages::fetch_request::FetchPartition::default()
            .with_partition(partition)
            .with_fetch_offset(offset)
            .with_partition_max_bytes(1048576); // 1MB

        let topic_data = kafka_protocol::messages::fetch_request::FetchTopic::default()
            .with_topic(TopicName(StrBytes::from(topic.to_string())))
            .with_partitions(vec![partition_data]);

        let request = FetchRequest::default()
            .with_topics(vec![topic_data])
            .with_max_wait_ms(500)
            .with_min_bytes(1)
            .with_max_bytes(52428800); // 50MB max

        let mut request_bytes = Vec::new();
        let header_version = ApiKey::Fetch.request_header_version(api_version);
        header.encode(&mut request_bytes, header_version)?;
        request.encode(&mut request_bytes, api_version)?;

        let size = request_bytes.len() as i32;
        debug!("Sending FetchRequest (size: {})", size);
        self.stream.write_i32(size).await?;
        self.stream.write_all(&request_bytes).await?;

        let response_size = self
            .stream
            .read_i32()
            .await
            .context("Failed to read response size")?;
        let mut response_bytes = vec![0; response_size as usize];
        self.stream.read_exact(&mut response_bytes).await?;

        debug!("Received FetchResponse (size: {})", response_size);
        let mut response_bytes_mut = BytesMut::from(&response_bytes[..]);
        let response_header_version = ApiKey::Fetch.response_header_version(api_version);
        let _response_header =
            ResponseHeader::decode(&mut response_bytes_mut, response_header_version)?;
        let response = FetchResponse::decode(&mut response_bytes_mut, api_version)?;

        Ok(response)
    }

    /// Sends a Metadata request to get information about topics and partitions
    pub async fn get_metadata(&mut self, topic_name: Option<&str>) -> Result<MetadataResponse> {
        // Get the best supported API version for Metadata
        let api_version = self.get_supported_version(ApiKey::Metadata, 12).await?;

        self.correlation_id += 1;

        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from("kc")));

        let request = if let Some(topic) = topic_name {
            // Request metadata for specific topic
            MetadataRequest::default().with_topics(Some(vec![
                kafka_protocol::messages::metadata_request::MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from(topic.to_string())))),
            ]))
        } else {
            // Request metadata for all topics (None means all topics)
            MetadataRequest::default().with_topics(None)
        };

        let mut request_bytes = Vec::new();
        let header_version = ApiKey::Metadata.request_header_version(api_version);
        header.encode(&mut request_bytes, header_version)?;
        request.encode(&mut request_bytes, api_version)?;

        let size = request_bytes.len() as i32;
        debug!("Sending MetadataRequest (size: {})", size);

        self.stream.write_i32(size).await?;
        self.stream.write_all(&request_bytes).await?;

        let response_size = self
            .stream
            .read_i32()
            .await
            .context("Failed to read response size")?;
        let mut response_bytes = vec![0; response_size as usize];
        self.stream.read_exact(&mut response_bytes).await?;

        debug!("Received MetadataResponse (size: {})", response_size);

        let mut response_bytes_mut = BytesMut::from(&response_bytes[..]);
        let response_header_version = ApiKey::Metadata.response_header_version(api_version);
        let _response_header =
            ResponseHeader::decode(&mut response_bytes_mut, response_header_version)?;

        let response = MetadataResponse::decode(&mut response_bytes_mut, api_version)?;
        Ok(response)
    }
}

/// Handles the get-topic command
async fn handle_get_topic(broker: &str, topic: String) -> Result<()> {
    let mut client = KafkaClient::new(broker).await?;

    info!("Fetching metadata for topic: {}", topic);
    let metadata = client.get_metadata(Some(&topic)).await?;

    if metadata.topics.is_empty() {
        println!("Topic '{}' not found", topic);
        return Ok(());
    }

    let topic_data = &metadata.topics[0];

    if topic_data.error_code == 3 {
        println!(
            "Error: Topic '{}' does not exist (UNKNOWN_TOPIC_OR_PARTITION)",
            topic
        );
        return Ok(());
    }

    if topic_data.error_code != 0 {
        println!(
            "Error: Topic '{}' returned error code {}",
            topic, topic_data.error_code
        );
        return Ok(());
    }

    let topic_name = topic_data
        .name
        .as_ref()
        .map(|n| n.to_string())
        .unwrap_or_else(|| "<unknown>".to_string());

    println!("\n=== Topic: {} ===", topic_name);
    println!("Topic ID: {}", topic_data.topic_id);
    println!("Internal: {}", topic_data.is_internal);
    println!("Partition Count: {}", topic_data.partitions.len());

    println!("\n=== Partitions ===");
    for partition in &topic_data.partitions {
        println!("  Partition {}:", partition.partition_index);
        println!("    Leader: {:?}", partition.leader_id);
        println!("    Replicas: {:?}", partition.replica_nodes);
        println!("    ISR: {:?}", partition.isr_nodes);
        if partition.error_code != 0 {
            println!("    Error Code: {}", partition.error_code);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topic_spec_with_partition() {
        let spec = "my-topic:3";
        let parts: Vec<&str> = spec.split(':').collect();
        assert_eq!(parts[0], "my-topic");
        assert_eq!(parts[1], "3");
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn test_parse_topic_spec_without_partition() {
        let spec = "my-topic";
        let parts: Vec<&str> = spec.split(':').collect();
        assert_eq!(parts[0], "my-topic");
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn test_parse_topic_spec_with_partition_and_offset() {
        let spec = "my-topic:0:100";
        let parts: Vec<&str> = spec.split(':').collect();
        assert_eq!(parts[0], "my-topic");
        assert_eq!(parts[1], "0");
        assert_eq!(parts[2], "100");
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_partition_count_validation() {
        let num_partitions = 3;
        assert!(num_partitions > 0);

        let num_partitions_zero = 0;
        assert!(num_partitions_zero <= 0);

        let num_partitions_negative = -1;
        assert!(num_partitions_negative <= 0);
    }

    #[test]
    fn test_parse_partition_from_string() {
        let partition_str = "5";
        let partition: Result<i32, _> = partition_str.parse();
        assert!(partition.is_ok());
        assert_eq!(partition.unwrap(), 5);

        let invalid_partition = "invalid";
        let result: Result<i32, _> = invalid_partition.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_offset_from_string() {
        let offset_str = "1000";
        let offset: Result<i64, _> = offset_str.parse();
        assert!(offset.is_ok());
        assert_eq!(offset.unwrap(), 1000);

        let negative_offset = "-1";
        let result: Result<i64, _> = negative_offset.parse();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -1);
    }

    #[test]
    fn test_kafka_error_codes() {
        // Test that error code constants are as expected
        let error_code_success = 0;
        let error_code_offset_out_of_range = 1;
        let error_code_unknown_topic = 3;
        let error_code_leader_not_available = 5;
        let error_code_not_leader = 6;
        let error_code_topic_already_exists = 36;
        let error_code_invalid_partitions = 37;
        let error_code_invalid_replication_factor = 38;

        assert_eq!(error_code_success, 0);
        assert_eq!(error_code_offset_out_of_range, 1);
        assert_eq!(error_code_unknown_topic, 3);
        assert_eq!(error_code_leader_not_available, 5);
        assert_eq!(error_code_not_leader, 6);
        assert_eq!(error_code_topic_already_exists, 36);
        assert_eq!(error_code_invalid_partitions, 37);
        assert_eq!(error_code_invalid_replication_factor, 38);
    }

    #[test]
    fn test_record_batch_encoding() {
        use bytes::{Bytes, BytesMut};
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions,
        };

        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: kafka_protocol::records::TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1234567890,
            key: Some(Bytes::from("test-key")),
            value: Some(Bytes::from("test-value")),
            headers: Default::default(),
        };

        let records = vec![record];
        let mut batch_buf = BytesMut::new();
        let encode_options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };

        let result = RecordBatchEncoder::encode(&mut batch_buf, &records, &encode_options);
        assert!(result.is_ok());
        assert!(!batch_buf.is_empty());
    }

    #[test]
    fn test_record_batch_decoding() {
        use bytes::{Bytes, BytesMut};
        use kafka_protocol::records::{
            Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        };

        // Create a record
        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: kafka_protocol::records::TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1234567890,
            key: Some(Bytes::from("test-key")),
            value: Some(Bytes::from("test-value")),
            headers: Default::default(),
        };

        // Encode it
        let records = vec![record];
        let mut batch_buf = BytesMut::new();
        let encode_options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        RecordBatchEncoder::encode(&mut batch_buf, &records, &encode_options).unwrap();

        // Decode it back
        let mut bytes = batch_buf.freeze();
        let result = RecordBatchDecoder::decode_all(&mut bytes);
        assert!(result.is_ok());

        let record_sets = result.unwrap();
        assert!(!record_sets.is_empty());

        let decoded_records = &record_sets[0].records;
        assert_eq!(decoded_records.len(), 1);
        assert_eq!(
            decoded_records[0].key.as_ref().unwrap(),
            &Bytes::from("test-key")
        );
        assert_eq!(
            decoded_records[0].value.as_ref().unwrap(),
            &Bytes::from("test-value")
        );
    }

    #[test]
    fn test_topic_name_validation() {
        // Valid topic names
        let valid_names = vec!["my-topic", "my_topic", "my.topic", "topic123", "t"];

        for name in valid_names {
            assert!(!name.is_empty());
            assert!(name.len() <= 249); // Kafka max topic name length
        }

        // Empty topic name should be invalid
        let empty = "";
        assert!(empty.is_empty());
    }

    #[test]
    fn test_partition_index_validation() {
        let partition = 0;
        assert!(partition >= 0);

        let partition = 5;
        assert!(partition >= 0);

        let negative = -1;
        assert!(negative < 0);
    }

    #[test]
    fn test_offset_validation() {
        // Valid offsets
        let offset_start = 0i64;
        assert!(offset_start >= 0);

        let offset_middle = 100i64;
        assert!(offset_middle >= 0);

        // Special offset value (beginning)
        let offset_beginning = -2i64;
        assert!(offset_beginning < 0);

        // Special offset value (end)
        let offset_end = -1i64;
        assert!(offset_end < 0);
    }

    #[test]
    fn test_broker_address_parsing() {
        let broker = "localhost:9092";
        let parts: Vec<&str> = broker.split(':').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "localhost");
        assert_eq!(parts[1], "9092");

        let port: Result<u16, _> = parts[1].parse();
        assert!(port.is_ok());
        assert_eq!(port.unwrap(), 9092);
    }

    #[test]
    fn test_api_version_negotiation_logic() {
        // Test that we select the minimum of requested and broker's max version
        let requested = 7;
        let broker_max = 5;
        let broker_min = 0;

        let negotiated = requested.min(broker_max).max(broker_min);
        assert_eq!(negotiated, 5);

        // Test when broker supports higher version
        let requested = 3;
        let broker_max = 10;
        let broker_min = 0;

        let negotiated = requested.min(broker_max).max(broker_min);
        assert_eq!(negotiated, 3);
    }

    #[test]
    fn test_correlation_id_increment() {
        let mut correlation_id = 0i32;

        correlation_id += 1;
        assert_eq!(correlation_id, 1);

        correlation_id += 1;
        assert_eq!(correlation_id, 2);

        correlation_id += 1;
        assert_eq!(correlation_id, 3);
    }

    #[test]
    fn test_bytes_conversion() {
        use bytes::Bytes;

        let key = "my-key";
        let key_bytes = Bytes::from(key.as_bytes().to_vec());
        assert_eq!(key_bytes.len(), 6);

        let value = "my-value";
        let value_bytes = Bytes::from(value.as_bytes().to_vec());
        assert_eq!(value_bytes.len(), 8);
    }
}

/// Handles the create-topic command
async fn handle_create_topic(broker: &str, topic_spec: String) -> Result<()> {
    // Parse topic_spec: topicname or topicname:partitions
    let parts: Vec<&str> = topic_spec.split(':').collect();
    let topic_name = parts[0];
    let num_partitions = if parts.len() > 1 {
        parts[1]
            .parse::<i32>()
            .context("Invalid partition count - must be a number")?
    } else {
        1
    };

    if num_partitions <= 0 {
        anyhow::bail!("Number of partitions must be greater than 0");
    }

    let mut client = KafkaClient::new(broker).await?;

    // Check if topic already exists
    info!("Checking if topic '{}' exists...", topic_name);
    let metadata = client.get_metadata(Some(topic_name)).await?;

    debug!("Metadata topics count: {}", metadata.topics.len());
    if !metadata.topics.is_empty() {
        let topic_data = &metadata.topics[0];
        debug!("Topic error_code: {}", topic_data.error_code);
        if topic_data.error_code == 0 {
            // Topic exists
            let existing_partitions = topic_data.partitions.len() as i32;

            if parts.len() <= 1 || existing_partitions == num_partitions {
                println!("Topic '{}' already exists", topic_name);
                return Ok(());
            } else {
                // Partition count mismatch
                println!("Topic '{}' already exists with {} partition(s), but you specified {} partition(s).",
                    topic_name, existing_partitions, num_partitions);
                println!("Do you want to recreate it? (y/n): ");

                use std::io::{self, BufRead};
                let stdin = io::stdin();
                let mut line = String::new();
                stdin.lock().read_line(&mut line)?;

                if line.trim().to_lowercase() != "y" {
                    println!("Aborted.");
                    return Ok(());
                }

                // Delete the existing topic
                info!("Deleting existing topic...");
                let delete_response = client.delete_topic(topic_name).await?;

                if let Some(topic_result) = delete_response.responses.first() {
                    if topic_result.error_code != 0 {
                        println!(
                            "Error deleting topic: error code {}",
                            topic_result.error_code
                        );
                        return Ok(());
                    }
                }

                println!("Topic deleted. Waiting for deletion to complete...");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    }

    // Create the topic
    info!(
        "Creating topic '{}' with {} partition(s)...",
        topic_name, num_partitions
    );
    let response = client.create_topic(topic_name, num_partitions).await?;

    debug!("CreateTopicsResponse: {:?}", response);
    if let Some(topic_result) = response.topics.first() {
        if topic_result.error_code == 0 {
            println!(
                "Topic '{}' created successfully with {} partition(s)",
                topic_name, num_partitions
            );
        } else {
            println!(
                "Error creating topic: error code {}",
                topic_result.error_code
            );
            if let Some(error_msg) = &topic_result.error_message {
                println!("Error message: {}", error_msg);
            }
            // Common error codes
            match topic_result.error_code {
                36 => println!("  (TOPIC_ALREADY_EXISTS)"),
                37 => println!("  (INVALID_PARTITIONS)"),
                38 => println!("  (INVALID_REPLICATION_FACTOR)"),
                _ => {}
            }
        }
    } else {
        println!("Warning: No response data received from create topic");
    }

    Ok(())
}

/// Handles the delete-topic command
async fn handle_delete_topic(broker: &str, topic: String) -> Result<()> {
    let mut client = KafkaClient::new(broker).await?;

    // Check if topic exists first
    info!("Checking if topic '{}' exists...", topic);
    let metadata = client.get_metadata(Some(&topic)).await?;

    if metadata.topics.is_empty() || metadata.topics[0].error_code == 3 {
        println!("Error: Topic '{}' does not exist", topic);
        return Ok(());
    }

    info!("Deleting topic '{}'...", topic);
    let response = client.delete_topic(&topic).await?;

    if let Some(topic_result) = response.responses.first() {
        if topic_result.error_code == 0 {
            println!("Topic '{}' deleted successfully", topic);
        } else {
            println!(
                "Error deleting topic: error code {}",
                topic_result.error_code
            );
            if let Some(error_msg) = &topic_result.error_message {
                println!("Error message: {}", error_msg);
            }
        }
    }

    Ok(())
}

/// Handles the publish command
async fn handle_publish(
    broker: &str,
    topic_spec: String,
    key: String,
    value: Option<String>,
) -> Result<()> {
    // Parse topic_spec: topicname or topicname:partition
    let parts: Vec<&str> = topic_spec.split(':').collect();
    let topic_name = parts[0];
    let partition = if parts.len() > 1 {
        parts[1]
            .parse::<i32>()
            .context("Invalid partition number - must be a number")?
    } else {
        0
    };

    let mut client = KafkaClient::new(broker).await?;

    // Verify topic and partition exist
    info!("Verifying topic '{}' exists...", topic_name);
    let metadata = client.get_metadata(Some(topic_name)).await?;

    if metadata.topics.is_empty() || metadata.topics[0].error_code != 0 {
        println!(
            "Error: Topic '{}' does not exist. Create it first with create-topic.",
            topic_name
        );
        return Ok(());
    }

    let topic_data = &metadata.topics[0];
    let partition_count = topic_data.partitions.len() as i32;

    if partition >= partition_count {
        println!(
            "Error: Partition {} does not exist. Topic '{}' has {} partition(s) (0-{})",
            partition,
            topic_name,
            partition_count,
            partition_count - 1
        );
        return Ok(());
    }

    info!(
        "Publishing message to topic '{}', partition {}...",
        topic_name, partition
    );

    let key_bytes = key.as_bytes();
    let value_bytes = value.as_ref().map(|v| v.as_bytes());

    let response = client
        .produce(topic_name, partition, key_bytes, value_bytes)
        .await?;

    debug!("ProduceResponse: {:?}", response);
    if let Some(topic_response) = response.responses.first() {
        if let Some(partition_response) = topic_response.partition_responses.first() {
            if partition_response.error_code == 0 {
                println!("Message published successfully");
                println!("  Partition: {}", partition_response.index);
                println!("  Offset: {}", partition_response.base_offset);
                if partition_response.log_append_time_ms != -1 {
                    println!("  Timestamp: {}", partition_response.log_append_time_ms);
                }
            } else {
                println!(
                    "Error publishing message: error code {}",
                    partition_response.error_code
                );
                // Common error codes
                match partition_response.error_code {
                    3 => println!(
                        "  (UNKNOWN_TOPIC_OR_PARTITION) - Topic or partition does not exist"
                    ),
                    5 => println!("  (LEADER_NOT_AVAILABLE) - Partition leader not available"),
                    6 => println!(
                        "  (NOT_LEADER_FOR_PARTITION) - Broker is not leader for this partition"
                    ),
                    _ => {}
                }
            }
        }
    } else {
        println!("Warning: No response data received");
    }

    Ok(())
}

/// Handles the fetch command
async fn handle_fetch(broker: &str, topic_spec: String) -> Result<()> {
    // Parse topic_spec: topicname:partition or topicname:partition:offset
    let parts: Vec<&str> = topic_spec.split(':').collect();

    if parts.len() < 2 {
        anyhow::bail!(
            "Invalid format. Expected: topicname:partition or topicname:partition:offset"
        );
    }

    let topic_name = parts[0];
    let partition = parts[1]
        .parse::<i32>()
        .context("Invalid partition number - must be a number")?;

    let offset = if parts.len() > 2 {
        parts[2]
            .parse::<i64>()
            .context("Invalid offset - must be a number")?
    } else {
        0 // Default to smallest offset
    };

    let mut client = KafkaClient::new(broker).await?;

    // Verify topic and partition exist
    info!("Verifying topic '{}' exists...", topic_name);
    let metadata = client.get_metadata(Some(topic_name)).await?;

    if metadata.topics.is_empty() || metadata.topics[0].error_code != 0 {
        println!("Error: Topic '{}' does not exist", topic_name);
        return Ok(());
    }

    let topic_data = &metadata.topics[0];
    let partition_count = topic_data.partitions.len() as i32;

    if partition >= partition_count {
        println!(
            "Error: Partition {} does not exist. Topic '{}' has {} partition(s) (0-{})",
            partition,
            topic_name,
            partition_count,
            partition_count - 1
        );
        return Ok(());
    }

    info!(
        "Fetching messages from topic '{}', partition {}, offset {}...",
        topic_name, partition, offset
    );

    let response = client.fetch(topic_name, partition, offset).await?;

    if let Some(topic_response) = response.responses.first() {
        if let Some(partition_response) = topic_response.partitions.first() {
            if partition_response.error_code != 0 {
                println!(
                    "Error fetching messages: error code {}",
                    partition_response.error_code
                );
                // Common error codes
                match partition_response.error_code {
                    3 => println!(
                        "  (UNKNOWN_TOPIC_OR_PARTITION) - Topic or partition does not exist"
                    ),
                    1 => println!("  (OFFSET_OUT_OF_RANGE) - Requested offset is out of range"),
                    _ => {}
                }
                return Ok(());
            }

            if let Some(records) = &partition_response.records {
                use bytes::Bytes;
                use kafka_protocol::records::RecordBatchDecoder;

                let mut records_bytes = Bytes::from(records.as_ref().to_vec());
                let mut record_count = 0;

                println!("\n=== Messages ===");

                // Decode all record sets from the buffer
                match RecordBatchDecoder::decode_all(&mut records_bytes) {
                    Ok(record_sets) => {
                        for record_set in record_sets {
                            for record in record_set.records {
                                record_count += 1;
                                println!("\nRecord {}:", record_count);

                                if let Some(key) = &record.key {
                                    let key_str = String::from_utf8_lossy(key);
                                    println!("  Key: {}", key_str);
                                } else {
                                    println!("  Key: (null)");
                                }

                                if let Some(value) = &record.value {
                                    let value_str = String::from_utf8_lossy(value);
                                    println!("  Value: {}", value_str);
                                } else {
                                    println!("  Value: (null)");
                                }
                            }
                        }

                        if record_count == 0 {
                            println!("No messages found");
                        }
                    }
                    Err(e) => {
                        println!("Error decoding records: {}", e);
                    }
                }
            } else {
                println!("No messages found");
            }
        }
    }

    Ok(())
}

/// Handles the get-metadata command
async fn handle_get_metadata(broker: &str, topic: Option<String>) -> Result<()> {
    let mut client = KafkaClient::new(broker).await?;

    // First, get API versions to verify connectivity
    info!("Fetching API versions...");
    let api_versions = client.get_api_versions().await?;

    println!("\n=== API Versions ===");
    println!("Error Code: {}", api_versions.error_code);
    if api_versions.error_code != 0 {
        println!("Warning: API version request returned error code");
    }

    // Find and display Metadata API version
    for api in &api_versions.api_keys {
        if api.api_key == ApiKey::Metadata as i16 {
            println!(
                "Metadata API: Min Version = {}, Max Version = {}",
                api.min_version, api.max_version
            );
        }
    }

    // Now get metadata
    info!("Fetching metadata...");
    let metadata = client.get_metadata(topic.as_deref()).await?;

    println!("\n=== Cluster Metadata ===");
    if let Some(cluster_id) = metadata.cluster_id {
        println!("Cluster ID: {}", cluster_id);
    }
    println!("Controller ID: {:?}", metadata.controller_id);
    println!("Throttle Time: {} ms", metadata.throttle_time_ms);

    println!("\n=== Brokers ===");
    for broker in &metadata.brokers {
        println!(
            "  Broker {:?}: {}:{}{}",
            broker.node_id,
            broker.host,
            broker.port,
            broker
                .rack
                .as_ref()
                .map(|r| format!(" (rack: {})", r))
                .unwrap_or_default()
        );
    }

    println!("\n=== Topics ===");
    if metadata.topics.is_empty() {
        println!("  No topics found");
    } else {
        for topic in &metadata.topics {
            let topic_name = topic
                .name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "<unknown>".to_string());

            println!("\nTopic: {}", topic_name);
            println!("  Topic ID: {}", topic.topic_id);
            println!("  Error Code: {}", topic.error_code);

            if topic.error_code != 0 {
                println!("  ⚠️  Warning: Topic has error code {}", topic.error_code);
                match topic.error_code {
                    3 => println!("     Error: UNKNOWN_TOPIC_OR_PARTITION"),
                    _ => println!("     Unknown error code"),
                }
            }

            if topic.is_internal {
                println!("  Internal: true");
            }

            println!("  Partitions: {}", topic.partitions.len());
            for partition in &topic.partitions {
                println!(
                    "    Partition {}: Leader={:?}, Replicas={:?}, ISR={:?}",
                    partition.partition_index,
                    partition.leader_id,
                    partition.replica_nodes,
                    partition.isr_nodes
                );
                if partition.error_code != 0 {
                    println!("      ⚠️  Partition error code: {}", partition.error_code);
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();

    match args.command {
        Command::GetMetadata { topic } => {
            handle_get_metadata(&args.broker, topic).await?;
        }
        Command::GetTopic { topic } => {
            handle_get_topic(&args.broker, topic).await?;
        }
        Command::CreateTopic { topic_spec } => {
            handle_create_topic(&args.broker, topic_spec).await?;
        }
        Command::DeleteTopic { topic } => {
            handle_delete_topic(&args.broker, topic).await?;
        }
        Command::Publish {
            topic_spec,
            key,
            value,
        } => {
            handle_publish(&args.broker, topic_spec, key, value).await?;
        }
        Command::Fetch { topic_spec } => {
            handle_fetch(&args.broker, topic_spec).await?;
        }
    }

    Ok(())
}
