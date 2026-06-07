use arrow_schema::{DataType, Field, TimeUnit};
use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, TimestampMillisecondArray};
use rdkafka::Message;
use rdkafka::message::BorrowedMessage;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct KafkaMetadata {
    partitions: Vec<i32>,
    offsets: Vec<i64>,
    timestamps: Vec<i64>,
}

impl KafkaMetadata {
    const COLUMN_NAME_PARTITION: &'static str = "__kafka_partition";
    const COLUMN_NAME_OFFSET: &'static str = "__kafka_offset";
    const COLUMN_NAME_TIMESTAMP: &'static str = "__kafka_timestamp";

    pub fn enrich_avro_fields(fields: &mut Vec<Arc<Field>>) {
        fields.push(Arc::new(Field::new(
            Self::COLUMN_NAME_PARTITION,
            DataType::Int32,
            false,
        )));
        fields.push(Arc::new(Field::new(
            Self::COLUMN_NAME_OFFSET,
            DataType::Int64,
            false,
        )));
        fields.push(Arc::new(Field::new(
            Self::COLUMN_NAME_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )));
    }

    pub fn copy_records_into(&mut self, record_columns: &mut Vec<ArrayRef>) {
        record_columns.push(Arc::new(Int32Array::from(self.partitions.clone())));
        record_columns.push(Arc::new(Int64Array::from(self.offsets.clone())));
        record_columns.push(Arc::new(TimestampMillisecondArray::from(
            self.timestamps.clone(),
        )));
    }

    // extract metadata from a Kafka message and add it to the internal vectors
    pub fn extract_metadata(&mut self, message: &BorrowedMessage) {
        self.partitions.push(message.partition());
        self.offsets.push(message.offset());
        let ts_millis = match message.timestamp() {
            rdkafka::message::Timestamp::CreateTime(ts) => ts,
            rdkafka::message::Timestamp::LogAppendTime(ts) => ts,
            _ => -1, // used for NotAvailable timestamp
        };
        self.timestamps.push(ts_millis);
    }

    pub fn clear(&mut self) {
        self.partitions.clear();
        self.offsets.clear();
        self.timestamps.clear();
    }
}
