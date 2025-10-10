use crate::error::Result;
use crate::row::ColumnArrayBuilder;
use crate::row::IdentityProp;
use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::mooncake_table::batch_id_counter::BatchIdCounter;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::shared_array::SharedRowBuffer;
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use crate::storage::storage_utils::{RawDeletionRecord, RecordLocation};
use arrow::array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct InMemoryBatch {
    pub(super) data: Option<Arc<RecordBatch>>,
    pub(super) deletions: BatchDeletionVector,
}

impl InMemoryBatch {
    pub fn new(max_rows_per_buffer: usize) -> Self {
        Self {
            data: None,
            deletions: BatchDeletionVector::new(max_rows_per_buffer),
        }
    }

    /// Get the number of records without filter.
    pub fn get_raw_record_number(&self) -> u64 {
        if let Some(record_batch) = &self.data {
            return record_batch.num_rows() as u64;
        }
        0
    }

    pub fn get_filtered_batch(&self) -> Result<Option<RecordBatch>> {
        if self.data.is_none() {
            return Ok(None);
        }
        let batch = self.deletions.apply_to_batch(self.data.as_ref().unwrap())?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(batch))
    }

    pub fn get_filtered_batch_with_limit(&self, row_limit: usize) -> Result<Option<RecordBatch>> {
        assert!(self.data.is_some());
        let batch = self
            .deletions
            .apply_to_batch(&self.data.as_ref().unwrap().slice(0, row_limit))?;
        Ok(Some(batch))
    }
}

#[derive(Debug, Clone)]
pub(super) struct BatchEntry {
    pub(super) id: u64,
    pub(super) batch: InMemoryBatch,
}

/// A streaming buffered writer for column-oriented data.
/// Creates new buffers when the current one is full and links them together.
pub struct ColumnStoreBuffer {
    /// The Arrow schema defining the structure of the data
    schema: Arc<Schema>,
    /// Maximum number of rows per buffer before creating a new one
    max_rows_per_buffer: usize,
    /// Collection of record batches including the current one
    in_memory_batches: Vec<BatchEntry>,
    /// Current batch being built
    current_batch_builder: Vec<ColumnArrayBuilder>,
    current_rows: SharedRowBuffer,
    /// Current row count in the current buffer
    current_row_count: usize,
    /// Batch ID allocator counter
    batch_id_counter: Arc<BatchIdCounter>,
}

impl ColumnStoreBuffer {
    /// Initialize a new column store buffer with the given schema and buffer size.
    ///
    pub fn new(
        schema: Arc<Schema>,
        max_rows_per_buffer: usize,
        batch_id_counter: Arc<BatchIdCounter>,
    ) -> Self {
        let current_batch_builder = schema
            .fields()
            .iter()
            .map(|field| ColumnArrayBuilder::new(field.data_type(), max_rows_per_buffer))
            .collect();

        // Get the initial batch ID from the counter
        // To avoid initial id conflict we need to acquire a unique id.
        // Notice, `next` returns the value before change
        let initial_id = batch_id_counter.get_and_next() + 1;

        Self {
            schema,
            max_rows_per_buffer,
            in_memory_batches: vec![BatchEntry {
                id: initial_id,
                batch: InMemoryBatch::new(max_rows_per_buffer),
            }],
            current_batch_builder,
            current_rows: SharedRowBuffer::new(max_rows_per_buffer),
            current_row_count: 0,
            batch_id_counter,
        }
    }

    /// Append a row for initial copy (append-only, no deletions).
    /// Skips MoonlinkRow creation and SharedRowBuffer since no deletions occur.
    /// Returns (batch_id, row_offset, optional_finished_batch).
    #[allow(clippy::type_complexity)]
    pub fn append_initial_copy_row(
        &mut self,
        values: Vec<RowValue>,
    ) -> Result<(u64, usize, Option<(u64, Arc<RecordBatch>)>)> {
        let mut new_batch: Option<(u64, Arc<RecordBatch>)> = None;
        // Check if we need to finalize the current batch
        if self.current_row_count >= self.max_rows_per_buffer {
            new_batch = self.finalize_current_batch()?;
        }

        // Append values directly to builders - no MoonlinkRow needed
        values.iter().enumerate().for_each(|(i, cell)| {
            let _res = self.current_batch_builder[i].append_value(cell);
            assert!(_res.is_ok());
        });
        self.current_row_count += 1;

        Ok((
            self.in_memory_batches.last().unwrap().id,
            self.current_row_count - 1,
            new_batch,
        ))
    }

    /// Append a row of data to the buffer. If the current buffer is full,
    /// finalize it and start a new one.
    ///
    #[allow(clippy::type_complexity)]
    pub(super) fn append_row(
        &mut self,
        row: MoonlinkRow,
    ) -> Result<(u64, usize, Option<(u64, Arc<RecordBatch>)>)> {
        let mut new_batch: Option<(u64, Arc<RecordBatch>)> = None;
        // Check if we need to finalize the current batch
        if self.current_row_count >= self.max_rows_per_buffer {
            new_batch = self.finalize_current_batch()?;
        }

        for (idx, cell) in row.values.iter().enumerate() {
            self.current_batch_builder[idx].append_value(cell)?;
        }
        self.current_row_count += 1;
        self.current_rows.push(row);

        Ok((
            self.in_memory_batches.last().unwrap().id,
            self.current_row_count - 1,
            new_batch,
        ))
    }

    /// Finalize the current batch, adding it to filled_batches and preparing for a new batch
    ///
    pub fn finalize_current_batch(&mut self) -> Result<Option<(u64, Arc<RecordBatch>)>> {
        if self.current_row_count == 0 {
            return Ok(None);
        }

        // Convert the current rows into a RecordBatch
        let columns = self
            .current_batch_builder
            .iter_mut()
            .zip(self.schema.fields())
            .map(|(builder, field)| {
                let finished = std::mem::replace(
                    builder,
                    ColumnArrayBuilder::new(field.data_type(), self.max_rows_per_buffer),
                );
                Arc::new(finished.finish(field.data_type())) as ArrayRef
            })
            .collect();

        let batch = Arc::new(RecordBatch::try_new(Arc::clone(&self.schema), columns)?);
        let last_batch = self.in_memory_batches.last_mut();
        last_batch.unwrap().batch.data = Some(batch.clone());
        let next_batch_id = self.batch_id_counter.get_and_next() + 1;
        self.in_memory_batches.push(BatchEntry {
            id: next_batch_id,
            batch: InMemoryBatch::new(self.max_rows_per_buffer),
        });
        // Reset the current batch
        self.current_row_count = 0;
        self.current_rows = SharedRowBuffer::new(self.max_rows_per_buffer);

        Ok(Some((next_batch_id - 1, batch)))
    }

    #[inline]
    pub fn check_identity(
        &self,
        record: &RawDeletionRecord,
        batch: &InMemoryBatch,
        offset: usize,
        identity: &IdentityProp,
    ) -> bool {
        if record.row_identity.is_some() && identity.requires_identity_check_in_mem_slice() {
            if let Some(batch) = &batch.data {
                record
                    .row_identity
                    .as_ref()
                    .unwrap()
                    .equals_record_batch_at_offset(batch, offset, identity)
            } else {
                record
                    .row_identity
                    .as_ref()
                    .unwrap()
                    .equals_moonlink_row(self.current_rows.get_row(offset), identity)
            }
        } else {
            true
        }
    }

    pub fn find_valid_row_by_record(
        &self,
        record: &RawDeletionRecord,
        record_location: &RecordLocation,
        identity: &IdentityProp,
    ) -> Option<(u64, usize)> {
        if let RecordLocation::MemoryBatch(batch_id, row_offset) = record_location {
            let idx = self
                .in_memory_batches
                .binary_search_by_key(batch_id, |x| x.id)
                .unwrap();
            if !self.in_memory_batches[idx]
                .batch
                .deletions
                .is_deleted(*row_offset)
                && self.check_identity(
                    record,
                    &self.in_memory_batches[idx].batch,
                    *row_offset,
                    identity,
                )
            {
                return Some((*batch_id, *row_offset));
            }
        }
        None
    }

    pub fn delete_row_by_record(
        &mut self,
        record: &RawDeletionRecord,
        record_location: &RecordLocation,
        identity: &IdentityProp,
    ) -> Option<(u64, usize)> {
        if let RecordLocation::MemoryBatch(batch_id, row_offset) = record_location {
            let idx = self
                .in_memory_batches
                .binary_search_by_key(batch_id, |x| x.id)
                .unwrap();
            if !self.in_memory_batches[idx]
                .batch
                .deletions
                .is_deleted(*row_offset)
                && self.check_identity(
                    record,
                    &self.in_memory_batches[idx].batch,
                    *row_offset,
                    identity,
                )
            {
                assert!(self.in_memory_batches[idx]
                    .batch
                    .deletions
                    .delete_row(*row_offset));
                return Some((*batch_id, *row_offset));
            }
        }
        None
    }

    pub(super) fn drain(&mut self) -> Vec<BatchEntry> {
        assert!(self.current_row_count == 0);
        let last = self.in_memory_batches.pop();
        let current_batch = std::mem::take(&mut self.in_memory_batches);
        self.in_memory_batches.push(last.unwrap());
        current_batch
    }

    pub(super) fn get_num_rows(&self) -> usize {
        (self.in_memory_batches.len() - 1) * self.max_rows_per_buffer + self.current_row_count
    }

    pub(super) fn get_commit_check_point(&self) -> RecordLocation {
        RecordLocation::MemoryBatch(
            self.in_memory_batches.last().unwrap().id,
            self.current_row_count,
        )
    }

    pub(super) fn get_latest_rows(&self) -> SharedRowBufferSnapshot {
        self.current_rows.get_snapshot()
    }

    #[must_use]
    pub(super) fn try_delete_at_pos(&mut self, pos: (u64, usize)) -> bool {
        let idx = self
            .in_memory_batches
            .binary_search_by_key(&pos.0, |x| x.id);
        if let Ok(index) = idx {
            let res = self.in_memory_batches[index]
                .batch
                .deletions
                .delete_row(pos.1);
            assert!(res);
            true
        } else {
            false
        }
    }
}

pub(super) fn create_batch_from_rows(
    rows: &[MoonlinkRow],
    schema: Arc<Schema>,
    deletions: &BatchDeletionVector,
) -> RecordBatch {
    let mut builders: Vec<ColumnArrayBuilder> = schema
        .fields()
        .iter()
        .map(|field| ColumnArrayBuilder::new(field.data_type(), rows.len()))
        .collect();
    for (i, row) in rows.iter().enumerate() {
        if !deletions.is_deleted(i) {
            for (j, _field) in schema.fields().iter().enumerate() {
                let _res = builders[j].append_value(&row.values[j]);
                assert!(_res.is_ok());
            }
        }
    }
    let columns: Vec<ArrayRef> = builders
        .into_iter()
        .zip(schema.fields())
        .map(|(builder, field)| builder.finish(field.data_type()))
        .collect();
    RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;
    use arrow::datatypes::{DataType, Field};
    use arrow_array::{Int16Array, Int32Array, StringArray, TimestampMicrosecondArray};
    use std::collections::HashMap;

    // TODO(hjiang): Add unit test for ColumnStoreBuffer with deletion, and check record batch content.
    #[test]
    fn test_column_store_buffer() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
            Field::new("age", DataType::Int16, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "3".to_string(),
            )])),
            Field::new(
                "event_date",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            )
            .with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "4".to_string(),
            )])),
        ]);

        let counter = BatchIdCounter::new(false);
        let start = counter.load() + 1;
        let mut buffer = ColumnStoreBuffer::new(Arc::new(schema.clone()), 2, Arc::new(counter));

        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(30),
            RowValue::Int64(1618876800000000),
        ]);

        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Jane".as_bytes().to_vec()),
            RowValue::Int32(25),
            RowValue::Int64(1618876800000000),
        ]);

        let row3 = MoonlinkRow::new(vec![
            RowValue::Int32(3),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
            RowValue::Int32(40),
            RowValue::Int64(1618876800000000),
        ]);

        buffer.append_row(row1)?;
        buffer.append_row(row2)?;

        // This should create a new buffer
        buffer.append_row(row3)?;
        buffer.finalize_current_batch()?;

        let batches = buffer.drain();
        assert_eq!(batches.len(), 2);

        // Check batch entry 1.
        let first_batch = &batches[0];
        assert_eq!(first_batch.id, start);
        assert!(first_batch
            .batch
            .deletions
            .collect_deleted_rows()
            .is_empty());
        let expected_record_batch = Arc::new(
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec![
                        "John".to_string(),
                        "Jane".to_string(),
                    ])),
                    Arc::new(Int16Array::from(vec![30, 25])),
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        1618876800000000,
                        1618876800000000,
                    ])),
                ],
            )
            .unwrap(),
        );
        assert_eq!(
            *first_batch.batch.data.as_ref().unwrap(),
            expected_record_batch
        );

        // Get filtered record batch.
        let filtered_batch = first_batch
            .batch
            .get_filtered_batch_with_limit(/*row_limit=*/ 1)
            .unwrap()
            .unwrap();
        let expected_record_batch = Arc::new(
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["John".to_string()])),
                    Arc::new(Int16Array::from(vec![30])),
                    Arc::new(TimestampMicrosecondArray::from(vec![1618876800000000])),
                ],
            )
            .unwrap(),
        );
        assert_eq!(Arc::new(filtered_batch), expected_record_batch);

        // Check batch entry 2.
        let second_batch = &batches[1];
        assert_eq!(second_batch.id, start + 1);
        assert!(second_batch
            .batch
            .deletions
            .collect_deleted_rows()
            .is_empty());
        let expected_record_batch = Arc::new(
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int32Array::from(vec![3])),
                    Arc::new(StringArray::from(vec!["Bob"])),
                    Arc::new(Int16Array::from(vec![40])),
                    Arc::new(TimestampMicrosecondArray::from(vec![1618876800000000])),
                ],
            )
            .unwrap(),
        );
        assert_eq!(
            *second_batch.batch.data.as_ref().unwrap(),
            expected_record_batch
        );

        Ok(())
    }
}
