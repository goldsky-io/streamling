// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Avro to Arrow array readers
//! Copied from DataFusion and modified to support:
//! - Decimals
//! - Operations on rows, not just files

use apache_avro::schema::RecordSchema;
use apache_avro::{
    AvroResult, Error as AvroError,
    schema::{Schema as AvroSchema, SchemaKind},
    types::Value,
};
use bigdecimal::BigDecimal;
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayData, ArrayDataBuilder, ArrayRef, BooleanBuilder, LargeBinaryBuilder,
    LargeStringArray, ListBuilder, NullArray, OffsetSizeTrait, PrimitiveArray, StringArray,
    StringBuilder, StringDictionaryBuilder, make_array,
};
use datafusion::arrow::array::{BinaryArray, FixedSizeBinaryArray, GenericListArray};
use datafusion::arrow::buffer::{Buffer, MutableBuffer};
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNumericType, ArrowPrimitiveType, DataType, Date32Type, Date64Type,
    Decimal128Type, Decimal256Type, Field, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, Schema, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    i256,
};
use datafusion::arrow::datatypes::{Fields, SchemaRef};
use datafusion::arrow::error::ArrowError::SchemaError;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::bit_util;
use datafusion::common::error::{DataFusionError, Result};
use num_bigint::BigInt;
use num_traits::NumCast;
use rdkafka::message::ToBytes;
use std::collections::BTreeMap;
use std::sync::Arc;

type RecordSlice<'a> = &'a [&'a Vec<(String, Value)>];

pub struct AvroArrowArrayReader {
    schema: SchemaRef,
    avro_schema: AvroSchema,
    projection: Option<Vec<String>>,
    schema_lookup: BTreeMap<String, usize>,
}

impl AvroArrowArrayReader {
    pub fn new(
        schema: SchemaRef,
        avro_schema: AvroSchema,
        projection: Option<Vec<String>>,
    ) -> Self {
        let schema_lookup = Self::schema_lookup(avro_schema.clone()).unwrap();

        Self {
            schema,
            avro_schema,
            projection,
            schema_lookup,
        }
    }

    pub fn schema_lookup(schema: AvroSchema) -> Result<BTreeMap<String, usize>> {
        match schema {
            AvroSchema::Record(RecordSchema {
                fields, mut lookup, ..
            }) => {
                for field in fields {
                    Self::child_schema_lookup(&field.name, &field.schema, &mut lookup)?;
                }
                Ok(lookup)
            }
            _ => Err(DataFusionError::ArrowError(
                Box::new(SchemaError(
                    "expected avro schema to be a record".to_string(),
                )),
                None,
            )),
        }
    }

    fn child_schema_lookup<'b>(
        parent_field_name: &str,
        schema: &AvroSchema,
        schema_lookup: &'b mut BTreeMap<String, usize>,
    ) -> Result<&'b BTreeMap<String, usize>> {
        match schema {
            AvroSchema::Union(us) => {
                let has_nullable = us
                    .find_schema_with_known_schemata::<apache_avro::Schema>(
                        &Value::Null,
                        None,
                        &None,
                    )
                    .is_some();
                let sub_schemas = us.variants();
                if has_nullable
                    && sub_schemas.len() == 2
                    && let Some(sub_schema) =
                        sub_schemas.iter().find(|&s| !matches!(s, AvroSchema::Null))
                {
                    Self::child_schema_lookup(parent_field_name, sub_schema, schema_lookup)?;
                }
            }
            AvroSchema::Record(RecordSchema { fields, lookup, .. }) => {
                lookup.iter().for_each(|(field_name, pos)| {
                    schema_lookup.insert(format!("{}.{}", parent_field_name, field_name), *pos);
                });

                for field in fields {
                    let sub_parent_field_name = format!("{}.{}", parent_field_name, field.name);
                    Self::child_schema_lookup(
                        &sub_parent_field_name,
                        &field.schema,
                        schema_lookup,
                    )?;
                }
            }
            AvroSchema::Array(schema) => {
                let sub_parent_field_name = format!("{}.element", parent_field_name);
                Self::child_schema_lookup(&sub_parent_field_name, &schema.items, schema_lookup)?;
            }
            _ => (),
        }
        Ok(schema_lookup)
    }

    /// Read the next batch of records
    pub fn read_batch(
        &mut self,
        rows: Vec<&Vec<(String, Value)>>,
    ) -> Option<ArrowResult<RecordBatch>> {
        let projection = self.projection.clone().unwrap_or_default();
        let arrays = self.build_struct_array(&rows, "", self.schema.fields(), &projection);
        let projected_fields = if projection.is_empty() {
            self.schema.fields().clone()
        } else {
            projection
                .iter()
                .filter_map(|name| self.schema.column_with_name(name))
                .map(|(_, field)| field.clone())
                .collect()
        };
        let projected_schema = Arc::new(Schema::new(projected_fields));
        Some(arrays.and_then(|arr| RecordBatch::try_new(projected_schema, arr)))
    }

    fn build_boolean_array(&self, rows: RecordSlice, col_name: &str) -> ArrayRef {
        let mut builder = BooleanBuilder::with_capacity(rows.len());
        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                if let Some(boolean) = resolve_boolean(value) {
                    builder.append_value(boolean)
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Arc::new(builder.finish())
    }

    fn build_primitive_array<T>(&self, rows: RecordSlice, col_name: &str) -> ArrayRef
    where
        T: ArrowNumericType + Resolver,
        T::Native: NumCast,
    {
        Arc::new(
            rows.iter()
                .map(|row| {
                    self.field_lookup(col_name, row)
                        .and_then(|value| resolve_item::<T>(value))
                })
                .collect::<PrimitiveArray<T>>(),
        )
    }

    #[inline(always)]
    fn build_string_dictionary_builder<T>(&self, row_len: usize) -> StringDictionaryBuilder<T>
    where
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        StringDictionaryBuilder::with_capacity(row_len, row_len, row_len)
    }

    fn build_wrapped_list_array(
        &self,
        rows: RecordSlice,
        col_name: &str,
        key_type: &DataType,
    ) -> ArrowResult<ArrayRef> {
        match *key_type {
            DataType::Int8 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int8Type>(&dtype, col_name, rows)
            }
            DataType::Int16 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int16Type>(&dtype, col_name, rows)
            }
            DataType::Int32 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int32Type>(&dtype, col_name, rows)
            }
            DataType::Int64 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int64Type>(&dtype, col_name, rows)
            }
            DataType::UInt8 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt8Type>(&dtype, col_name, rows)
            }
            DataType::UInt16 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt16Type>(&dtype, col_name, rows)
            }
            DataType::UInt32 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt32Type>(&dtype, col_name, rows)
            }
            DataType::UInt64 => {
                let dtype =
                    DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt64Type>(&dtype, col_name, rows)
            }
            ref e => Err(SchemaError(format!(
                "Data type is currently not supported for dictionaries in list : {e:?}"
            ))),
        }
    }

    #[inline(always)]
    fn list_array_string_array_builder<D>(
        &self,
        data_type: &DataType,
        col_name: &str,
        rows: RecordSlice,
    ) -> ArrowResult<ArrayRef>
    where
        D: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Utf8 => {
                let values_builder = StringBuilder::with_capacity(rows.len(), 5);
                Box::new(ListBuilder::new(values_builder))
            }
            DataType::Dictionary(_, _) => {
                let values_builder = self.build_string_dictionary_builder::<D>(rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            e => {
                return Err(SchemaError(format!(
                    "Nested list data builder type is not supported: {e:?}"
                )));
            }
        };

        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                let value = maybe_resolve_union(value);
                // For array elements, construct the field path
                let element_field_path = format!("{}.element", col_name);
                // value can be an array or a scalar
                let vals: Vec<Option<String>> = if let Value::String(v) = value {
                    vec![Some(v.to_string())]
                } else if let Value::Array(n) = value {
                    n.iter()
                        .map(|item| self.resolve_string(item, &element_field_path))
                        .collect::<ArrowResult<Vec<Option<String>>>>()?
                } else if let Value::Null = value {
                    vec![None]
                } else if !matches!(value, Value::Record(_)) {
                    vec![self.resolve_string(value, &element_field_path)?]
                } else {
                    return Err(SchemaError(
                        "Only scalars are currently supported in Avro arrays".to_string(),
                    ));
                };

                // TODO: ARROW-10335: APIs of dictionary arrays and others are different. Unify
                // them.
                match data_type {
                    DataType::Utf8 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<StringBuilder>>()
                            .ok_or_else(||SchemaError(
                                "Cast failed for ListBuilder<StringBuilder> during nested data parsing".to_string(),
                            ))?;
                        for val in vals {
                            if let Some(v) = val {
                                builder.values().append_value(&v)
                            } else {
                                builder.values().append_null()
                            };
                        }

                        // Append to the list
                        builder.append(true);
                    }
                    DataType::Dictionary(_, _) => {
                        let builder = builder.as_any_mut().downcast_mut::<ListBuilder<StringDictionaryBuilder<D>>>().ok_or_else(||SchemaError(
                            "Cast failed for ListBuilder<StringDictionaryBuilder> during nested data parsing".to_string(),
                        ))?;
                        for val in vals {
                            if let Some(v) = val {
                                let _ = builder.values().append(&v)?;
                            } else {
                                builder.values().append_null()
                            };
                        }

                        // Append to the list
                        builder.append(true);
                    }
                    e => {
                        return Err(SchemaError(format!(
                            "Nested list data builder type is not supported: {e:?}"
                        )));
                    }
                }
            }
        }

        Ok(builder.finish() as ArrayRef)
    }

    #[inline(always)]
    fn build_dictionary_array<T>(&self, rows: RecordSlice, col_name: &str) -> ArrowResult<ArrayRef>
    where
        T::Native: NumCast,
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: StringDictionaryBuilder<T> =
            self.build_string_dictionary_builder(rows.len());
        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                if let Ok(Some(str_v)) = self.resolve_string(value, col_name) {
                    builder.append(str_v).map(drop)?
                } else {
                    builder.append_null()
                }
            } else {
                builder.append_null()
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    #[inline(always)]
    fn build_string_dictionary_array(
        &self,
        rows: RecordSlice,
        col_name: &str,
        key_type: &DataType,
        value_type: &DataType,
    ) -> ArrowResult<ArrayRef> {
        if let DataType::Utf8 = *value_type {
            match *key_type {
                DataType::Int8 => self.build_dictionary_array::<Int8Type>(rows, col_name),
                DataType::Int16 => self.build_dictionary_array::<Int16Type>(rows, col_name),
                DataType::Int32 => self.build_dictionary_array::<Int32Type>(rows, col_name),
                DataType::Int64 => self.build_dictionary_array::<Int64Type>(rows, col_name),
                DataType::UInt8 => self.build_dictionary_array::<UInt8Type>(rows, col_name),
                DataType::UInt16 => self.build_dictionary_array::<UInt16Type>(rows, col_name),
                DataType::UInt32 => self.build_dictionary_array::<UInt32Type>(rows, col_name),
                DataType::UInt64 => self.build_dictionary_array::<UInt64Type>(rows, col_name),
                _ => Err(SchemaError("unsupported dictionary key type".to_string())),
            }
        } else {
            Err(SchemaError(
                "dictionary types other than UTF-8 not yet supported".to_string(),
            ))
        }
    }

    /// Build a nested GenericListArray from a list of unnested `Value`s
    fn build_nested_list_array<OffsetSize: OffsetSizeTrait>(
        &self,
        parent_field_name: &str,
        rows: &[&Value],
        list_field: &Field,
    ) -> ArrowResult<ArrayRef> {
        // build list offsets
        let mut cur_offset = OffsetSize::zero();
        let list_len = rows.len();
        let num_list_bytes = bit_util::ceil(list_len, 8);
        let mut offsets = Vec::with_capacity(list_len + 1);
        let mut list_nulls = MutableBuffer::from_len_zeroed(num_list_bytes);
        offsets.push(cur_offset);
        rows.iter().enumerate().for_each(|(i, v)| {
            // TODO: unboxing Union(Array(Union(...))) should probably be done earlier
            let v = maybe_resolve_union(v);
            if let Value::Array(a) = v {
                cur_offset += OffsetSize::from_usize(a.len()).unwrap();
                bit_util::set_bit(&mut list_nulls, i);
            } else if let Value::Null = v {
                // value is null, not incremented
            } else {
                cur_offset += OffsetSize::one();
            }
            offsets.push(cur_offset);
        });
        let valid_len = cur_offset.to_usize().unwrap();
        let array_data = match list_field.data_type() {
            DataType::Null => NullArray::new(valid_len).into_data(),
            DataType::Boolean => {
                let num_bytes = bit_util::ceil(valid_len, 8);
                let mut bool_values = MutableBuffer::from_len_zeroed(num_bytes);
                let mut bool_nulls = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                let mut curr_index = 0;
                rows.iter().for_each(|v| {
                    if let Value::Array(vs) = v {
                        vs.iter().for_each(|value| {
                            if let Value::Boolean(child) = value {
                                // if valid boolean, append value
                                if *child {
                                    bit_util::set_bit(&mut bool_values, curr_index);
                                }
                            } else {
                                // null slot
                                bit_util::unset_bit(&mut bool_nulls, curr_index);
                            }
                            curr_index += 1;
                        });
                    }
                });
                ArrayData::builder(list_field.data_type().clone())
                    .len(valid_len)
                    .add_buffer(bool_values.into())
                    .null_bit_buffer(Some(bool_nulls.into()))
                    .build()
                    .unwrap()
            }
            DataType::Int8 => self.read_primitive_list_values::<Int8Type>(rows),
            DataType::Int16 => self.read_primitive_list_values::<Int16Type>(rows),
            DataType::Int32 => self.read_primitive_list_values::<Int32Type>(rows),
            DataType::Int64 => self.read_primitive_list_values::<Int64Type>(rows),
            DataType::UInt8 => self.read_primitive_list_values::<UInt8Type>(rows),
            DataType::UInt16 => self.read_primitive_list_values::<UInt16Type>(rows),
            DataType::UInt32 => self.read_primitive_list_values::<UInt32Type>(rows),
            DataType::UInt64 => self.read_primitive_list_values::<UInt64Type>(rows),
            DataType::Float16 => return Err(SchemaError("Float16 not supported".to_string())),
            DataType::Float32 => self.read_primitive_list_values::<Float32Type>(rows),
            DataType::Float64 => self.read_primitive_list_values::<Float64Type>(rows),
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => {
                return Err(SchemaError(
                    "Temporal types are not yet supported, see ARROW-4803".to_string(),
                ));
            }
            DataType::Utf8 => {
                let element_field_path = format!("{}.{}", parent_field_name, list_field.name());
                self.flatten_string_values(rows, &element_field_path)?
                    .into_iter()
                    .collect::<StringArray>()
                    .into_data()
            }
            DataType::LargeUtf8 => {
                let element_field_path = format!("{}.{}", parent_field_name, list_field.name());
                self.flatten_string_values(rows, &element_field_path)?
                    .into_iter()
                    .collect::<LargeStringArray>()
                    .into_data()
            }
            DataType::List(field) => {
                let child = self.build_nested_list_array::<i32>(
                    parent_field_name,
                    &flatten_values(rows),
                    field,
                )?;
                child.to_data()
            }
            DataType::LargeList(field) => {
                let child = self.build_nested_list_array::<i64>(
                    parent_field_name,
                    &flatten_values(rows),
                    field,
                )?;
                child.to_data()
            }
            DataType::Struct(fields) => {
                // extract list values, with non-lists converted to Value::Null
                let array_item_count = rows
                    .iter()
                    .map(|row| match maybe_resolve_union(row) {
                        Value::Array(values) => values.len(),
                        _ => 1,
                    })
                    .sum();
                let num_bytes = bit_util::ceil(array_item_count, 8);
                let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                let mut struct_index = 0;
                let null_struct_array = vec![("null".to_string(), Value::Null)];
                let rows: Vec<&Vec<(String, Value)>> = rows
                    .iter()
                    .map(|v| maybe_resolve_union(v))
                    .flat_map(|row| {
                        if let Value::Array(values) = row {
                            values
                                .iter()
                                .map(maybe_resolve_union)
                                .map(|v| match v {
                                    Value::Record(record) => {
                                        bit_util::set_bit(&mut null_buffer, struct_index);
                                        struct_index += 1;
                                        record
                                    }
                                    Value::Null => {
                                        struct_index += 1;
                                        &null_struct_array
                                    }
                                    other => panic!("expected Record, got {other:?}"),
                                })
                                .collect::<Vec<&Vec<(String, Value)>>>()
                        } else {
                            struct_index += 1;
                            vec![&null_struct_array]
                        }
                    })
                    .collect();

                let sub_parent_field_name = format!("{}.{}", parent_field_name, list_field.name());
                let arrays = self.build_struct_array(&rows, &sub_parent_field_name, fields, &[])?;
                let data_type = DataType::Struct(fields.clone());
                ArrayDataBuilder::new(data_type)
                    .len(rows.len())
                    .null_bit_buffer(Some(null_buffer.into()))
                    .child_data(arrays.into_iter().map(|a| a.to_data()).collect())
                    .build()
                    .unwrap()
            }
            datatype => {
                return Err(SchemaError(format!(
                    "Nested list of {datatype:?} not supported"
                )));
            }
        };
        // build list
        let list_data = ArrayData::builder(DataType::List(Arc::new(list_field.clone())))
            .len(list_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(array_data)
            .null_bit_buffer(Some(list_nulls.into()))
            .build()
            .unwrap();
        Ok(Arc::new(GenericListArray::<OffsetSize>::from(list_data)))
    }

    /// Builds the child values of a `StructArray`, falling short of constructing the StructArray.
    /// The function does not construct the StructArray as some callers would want the child arrays.
    ///
    /// *Note*: The function is recursive, and will read nested structs.
    ///
    /// If `projection` is not empty, then all values are returned. The first level of projection
    /// occurs at the `RecordBatch` level. No further projection currently occurs, but would be
    /// useful if plucking values from a struct, e.g. getting `a.b.c.e` from `a.b.c.{d, e}`.
    fn build_struct_array(
        &self,
        rows: RecordSlice,
        parent_field_name: &str,
        struct_fields: &Fields,
        projection: &[String],
    ) -> ArrowResult<Vec<ArrayRef>> {
        let arrays: ArrowResult<Vec<ArrayRef>> = struct_fields
            .iter()
            .filter(|field| projection.is_empty() || projection.contains(field.name()))
            .map(|field| {
                let field_path = if parent_field_name.is_empty() {
                    field.name().to_string()
                } else {
                    format!("{}.{}", parent_field_name, field.name())
                };
                let arr = match field.data_type() {
                    DataType::Null => Arc::new(NullArray::new(rows.len())) as ArrayRef,
                    DataType::Boolean => self.build_boolean_array(rows, &field_path),
                    DataType::Float64 => {
                        self.build_primitive_array::<Float64Type>(rows, &field_path)
                    }
                    DataType::Float32 => {
                        self.build_primitive_array::<Float32Type>(rows, &field_path)
                    }
                    DataType::Int64 => self.build_primitive_array::<Int64Type>(rows, &field_path),
                    DataType::Int32 => self.build_primitive_array::<Int32Type>(rows, &field_path),
                    DataType::Int16 => self.build_primitive_array::<Int16Type>(rows, &field_path),
                    DataType::Int8 => self.build_primitive_array::<Int8Type>(rows, &field_path),
                    DataType::UInt64 => self.build_primitive_array::<UInt64Type>(rows, &field_path),
                    DataType::UInt32 => self.build_primitive_array::<UInt32Type>(rows, &field_path),
                    DataType::UInt16 => self.build_primitive_array::<UInt16Type>(rows, &field_path),
                    DataType::UInt8 => self.build_primitive_array::<UInt8Type>(rows, &field_path),
                    // TODO: this is incomplete
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => {
                            self.build_primitive_array::<TimestampSecondType>(rows, &field_path)
                        }
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<TimestampMicrosecondType>(rows, &field_path),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<TimestampMillisecondType>(rows, &field_path),
                        TimeUnit::Nanosecond => {
                            self.build_primitive_array::<TimestampNanosecondType>(rows, &field_path)
                        }
                    },
                    DataType::Date64 => self.build_primitive_array::<Date64Type>(rows, &field_path),
                    DataType::Date32 => self.build_primitive_array::<Date32Type>(rows, &field_path),
                    DataType::Time64(unit) => {
                        match unit {
                            TimeUnit::Microsecond => self
                                .build_primitive_array::<Time64MicrosecondType>(rows, &field_path),
                            TimeUnit::Nanosecond => self
                                .build_primitive_array::<Time64NanosecondType>(rows, &field_path),
                            t => {
                                return Err(SchemaError(format!(
                                    "TimeUnit {t:?} not supported with Time64"
                                )));
                            }
                        }
                    }
                    DataType::Time32(unit) => match unit {
                        TimeUnit::Second => {
                            self.build_primitive_array::<Time32SecondType>(rows, &field_path)
                        }
                        TimeUnit::Millisecond => {
                            self.build_primitive_array::<Time32MillisecondType>(rows, &field_path)
                        }
                        t => {
                            return Err(SchemaError(format!(
                                "TimeUnit {t:?} not supported with Time32"
                            )));
                        }
                    },
                    DataType::Utf8 | DataType::LargeUtf8 => Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = self.field_lookup(&field_path, row);
                                match maybe_value {
                                    None => Ok(None),
                                    Some(v) => self.resolve_string(v, &field_path),
                                }
                            })
                            .collect::<ArrowResult<StringArray>>()?,
                    ) as ArrayRef,
                    DataType::LargeBinary
                        if crate::types::decimal_arb::DecimalArbType::is_decimal_arb_field(
                            field,
                        ) =>
                    {
                        // Decode Avro decimal values into canonical decimal_arb bytes
                        // at the column's declared scale.
                        let (_, scale) =
                            crate::types::decimal_arb::DecimalArbType::precision_scale_from_field(
                                field,
                            )
                            .ok_or_else(|| {
                                SchemaError(format!(
                                    "decimal_arb field '{}' missing precision/scale metadata",
                                    field.name()
                                ))
                            })?;
                        let mut builder = LargeBinaryBuilder::with_capacity(rows.len(), 0);
                        for row in rows {
                            // Match `build_decimal_array`'s nullability semantics:
                            // a missing field on a non-nullable column is a schema
                            // error, not a silent null. The prior `.unwrap()` here
                            // panicked unconditionally for any missing field, which
                            // was wrong for nullable columns; this resolves both.
                            match self.field_lookup(&field_path, row) {
                                Some(v) => match resolve_decimal_arb_canonical_bytes(v, scale)? {
                                    Some(bytes) => builder.append_value(&bytes),
                                    None => {
                                        if !field.is_nullable() {
                                            return Err(SchemaError(format!(
                                                "Unexpected null value for non-nullable decimal_arb field '{}'",
                                                field.name()
                                            )));
                                        }
                                        builder.append_null();
                                    }
                                },
                                None => {
                                    if !field.is_nullable() {
                                        return Err(SchemaError(format!(
                                            "Missing non-nullable decimal_arb field '{}'",
                                            field.name()
                                        )));
                                    }
                                    builder.append_null();
                                }
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    DataType::Binary | DataType::LargeBinary => Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = self.field_lookup(&field_path, row);
                                maybe_value.and_then(resolve_bytes)
                            })
                            .collect::<BinaryArray>(),
                    ) as ArrayRef,
                    DataType::FixedSizeBinary(size) if *size != 32 => {
                        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            rows.iter().map(|row| {
                                let maybe_value = self.field_lookup(&field_path, row);
                                maybe_value.and_then(|v| resolve_fixed(v, *size as usize))
                            }),
                            *size,
                        )?) as ArrayRef
                    }
                    DataType::List(list_field) => {
                        match list_field.data_type() {
                            DataType::Dictionary(key_ty, _) => {
                                self.build_wrapped_list_array(rows, &field_path, key_ty)?
                            }
                            _ => {
                                // extract rows by name
                                let extracted_rows = rows
                                    .iter()
                                    .map(|row| {
                                        self.field_lookup(&field_path, row).unwrap_or(&Value::Null)
                                    })
                                    .collect::<Vec<&Value>>();
                                self.build_nested_list_array::<i32>(
                                    &field_path,
                                    &extracted_rows,
                                    list_field,
                                )?
                            }
                        }
                    }
                    DataType::Dictionary(key_ty, val_ty) => {
                        self.build_string_dictionary_array(rows, &field_path, key_ty, val_ty)?
                    }
                    DataType::Struct(fields) => {
                        let len = rows.len();
                        let num_bytes = bit_util::ceil(len, 8);
                        let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                        let empty_vec = vec![];
                        let struct_rows = rows
                            .iter()
                            .enumerate()
                            .map(|(i, row)| (i, self.field_lookup(&field_path, row)))
                            .map(|(i, v)| {
                                let v = v.map(maybe_resolve_union);
                                match v {
                                    Some(Value::Record(value)) => {
                                        bit_util::set_bit(&mut null_buffer, i);
                                        value
                                    }
                                    None | Some(Value::Null) => &empty_vec,
                                    other => {
                                        panic!("expected struct got {other:?}");
                                    }
                                }
                            })
                            .collect::<Vec<&Vec<(String, Value)>>>();
                        let arrays =
                            self.build_struct_array(&struct_rows, &field_path, fields, &[])?;
                        // construct a struct array's data in order to set null buffer
                        let data_type = DataType::Struct(fields.clone());
                        let data = ArrayDataBuilder::new(data_type)
                            .len(len)
                            .null_bit_buffer(Some(null_buffer.into()))
                            .child_data(arrays.into_iter().map(|a| a.to_data()).collect())
                            .build()?;
                        make_array(data)
                    }
                    DataType::Decimal128(precision, scale) => self
                        .build_decimal_array::<Decimal128Type>(
                            rows,
                            &field_path,
                            field,
                            DataType::Decimal128(*precision, *scale),
                            resolve_decimal,
                        )?,
                    DataType::Decimal256(precision, scale) => self
                        .build_decimal_array::<Decimal256Type>(
                            rows,
                            &field_path,
                            field,
                            DataType::Decimal256(*precision, *scale),
                            resolve_decimal_256,
                        )?,
                    DataType::FixedSizeBinary(32)
                        if crate::types::u256::U256Type::is_u256_field(field) =>
                    {
                        // Convert decimal values to U256 (FixedSizeBinary(32))
                        let values: Vec<Option<[u8; 32]>> = rows
                            .iter()
                            .map(|row| self.field_lookup(&field_path, row).unwrap())
                            .map(resolve_u256)
                            .collect::<ArrowResult<Vec<_>>>()?;

                        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            values.into_iter(),
                            32,
                        )?) as ArrayRef
                    }
                    DataType::FixedSizeBinary(32)
                        if crate::types::i256::I256Type::is_i256_field(field) =>
                    {
                        // Convert decimal values to I256 (FixedSizeBinary(32))
                        let values: Vec<Option<[u8; 32]>> = rows
                            .iter()
                            .map(|row| self.field_lookup(&field_path, row).unwrap())
                            .map(resolve_i256)
                            .collect::<ArrowResult<Vec<_>>>()?;

                        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            values.into_iter(),
                            32,
                        )?) as ArrayRef
                    }
                    DataType::FixedSizeBinary(32) => {
                        // Regular FixedSizeBinary(32) without metadata
                        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            rows.iter().map(|row| {
                                let maybe_value = self.field_lookup(&field_path, row);
                                maybe_value.and_then(|v| resolve_fixed(v, 32))
                            }),
                            32,
                        )?) as ArrayRef
                    }
                    _ => {
                        return Err(SchemaError(format!(
                            "type {:?} not supported",
                            field.data_type()
                        )));
                    }
                };
                Ok(arr)
            })
            .collect();
        arrays
    }

    /// Read the primitive list's values into ArrayData
    fn read_primitive_list_values<T>(&self, rows: &[&Value]) -> ArrayData
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: NumCast,
    {
        let values = rows
            .iter()
            .flat_map(|row| {
                let row = maybe_resolve_union(row);
                if let Value::Array(values) = row {
                    values
                        .iter()
                        .map(resolve_item::<T>)
                        .collect::<Vec<Option<T::Native>>>()
                } else if let Some(f) = resolve_item::<T>(row) {
                    vec![Some(f)]
                } else {
                    vec![]
                }
            })
            .collect::<Vec<Option<T::Native>>>();
        let array = values.iter().collect::<PrimitiveArray<T>>();
        array.to_data()
    }

    fn field_lookup<'b>(&self, name: &str, row: &'b [(String, Value)]) -> Option<&'b Value> {
        self.schema_lookup
            .get(name)
            .and_then(|i| row.get(*i))
            .map(|o| &o.1)
    }

    /// Look up the Avro schema for a field by path (e.g., "field_name" or "parent.child")
    fn avro_field_schema_lookup(&self, field_path: &str) -> Option<&AvroSchema> {
        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current_schema = &self.avro_schema;

        for part in parts {
            match current_schema {
                AvroSchema::Record(record_schema) => {
                    // Find the field by name
                    if let Some(field) = record_schema.fields.iter().find(|f| f.name == part) {
                        current_schema = &field.schema;
                    } else {
                        return None;
                    }
                }
                AvroSchema::Union(union_schema) => {
                    // For unions, try to find a non-null schema that matches
                    for variant in union_schema.variants() {
                        if let AvroSchema::Record(record_schema) = variant
                            && let Some(field) =
                                record_schema.fields.iter().find(|f| f.name == part)
                        {
                            current_schema = &field.schema;
                            break;
                        }
                    }
                }
                _ => return None,
            }
        }

        Some(current_schema)
    }

    /// Extract decimal scale from Avro schema if it's a decimal type
    fn get_decimal_scale(&self, field_path: &str) -> Option<usize> {
        let schema = self.avro_field_schema_lookup(field_path)?;

        // Handle union types (nullable decimals)
        let schema = match schema {
            AvroSchema::Union(union_schema) => {
                // Find the decimal schema in the union (skip Null)
                union_schema
                    .variants()
                    .iter()
                    .find(|s| matches!(s, AvroSchema::Decimal(_)))
                    .unwrap_or(schema)
            }
            _ => schema,
        };

        // Check if it's a decimal logical type
        match schema {
            AvroSchema::Decimal(decimal_schema) => Some(decimal_schema.scale),
            _ => None,
        }
    }

    /// Resolve a value to string, with automatic decimal formatting using schema context.
    /// For decimal values, looks up the scale from the Avro schema and formats accordingly.
    /// For other types, performs standard string conversion.
    fn resolve_string(&self, v: &Value, field_path: &str) -> ArrowResult<Option<String>> {
        let v = if let Value::Union(_, b) = v { b } else { v };
        match v {
            Value::String(s) => Ok(Some(s.clone())),
            Value::Bytes(bytes) => String::from_utf8(bytes.to_vec())
                .map_err(AvroError::ConvertToUtf8)
                .map(Some),
            Value::Enum(_, s) => Ok(Some(s.clone())),
            Value::Null => Ok(None),
            Value::Decimal(decimal_value) => {
                // Look up scale from Avro schema, defaulting to 0 if not found
                let scale = self.get_decimal_scale(field_path).unwrap_or(0);
                let result = format_decimal_with_scale(decimal_value, scale);
                // Convert ArrowError to AvroError - use GetString as it's the closest match
                result.map_err(|e| AvroError::GetString(Value::String(format!("{:?}", e)).into()))
            }
            other => Err(AvroError::GetString(other.into())),
        }
        .map_err(|e| SchemaError(format!("expected resolvable string : {e:?}")))
    }

    fn build_decimal_array<T>(
        &self,
        rows: RecordSlice,
        field_path: &str,
        field: &Field,
        data_type: DataType,
        resolver: impl Fn(&Value) -> ArrowResult<Option<T::Native>>,
    ) -> ArrowResult<ArrayRef>
    where
        T: ArrowPrimitiveType,
    {
        let array = rows
            .iter()
            .map(|row| match self.field_lookup(field_path, row) {
                Some(v) => {
                    let result = resolver(v)?;
                    if result.is_none() && !field.is_nullable() {
                        return Err(SchemaError(format!(
                            "Unexpected null value for non-nullable decimal field '{}'",
                            field_path
                        )));
                    }
                    Ok(result)
                }
                None => {
                    if !field.is_nullable() {
                        return Err(SchemaError(format!(
                            "Missing non-nullable decimal field '{}'",
                            field_path
                        )));
                    }
                    Ok(None)
                }
            })
            .collect::<ArrowResult<PrimitiveArray<T>>>()?;

        Ok(Arc::new(array.with_data_type(data_type)) as ArrayRef)
    }

    /// Flattens a list into string values, dropping Value::Null in the process.
    /// This is useful for interpreting any Avro array as string, dropping nulls.
    /// Uses schema context for proper decimal formatting.
    #[inline]
    fn flatten_string_values(
        &self,
        values: &[&Value],
        field_path: &str,
    ) -> ArrowResult<Vec<Option<String>>> {
        values
            .iter()
            .flat_map(|row| {
                let row = maybe_resolve_union(row);
                if let Value::Array(values) = row {
                    values
                        .iter()
                        .map(|s| self.resolve_string(s, field_path))
                        .collect::<Vec<ArrowResult<Option<String>>>>()
                } else if let Value::Null = row {
                    vec![]
                } else {
                    vec![self.resolve_string(row, field_path)]
                }
            })
            .collect::<ArrowResult<Vec<Option<_>>>>()
    }
}

/// Flattens a list of Avro values, by flattening lists, and treating all other values as
/// single-value lists.
/// This is used to read into nested lists (list of list, list of struct) and non-dictionary lists.
#[inline]
fn flatten_values<'a>(values: &[&'a Value]) -> Vec<&'a Value> {
    values
        .iter()
        .flat_map(|row| {
            let v = maybe_resolve_union(row);
            if let Value::Array(values) = v {
                values.iter().collect()
            } else {
                // we interpret a scalar as a single-value list to minimise data loss
                vec![v]
            }
        })
        .collect()
}

/// Format a decimal value as a string with the given scale.
/// The decimal is stored as big-endian bytes representing a signed integer.
/// The scale indicates how many decimal places there are.
fn format_decimal_with_scale(
    decimal: &apache_avro::Decimal,
    scale: usize,
) -> ArrowResult<Option<String>> {
    // Convert decimal to bytes using the public API
    let bytes = <Vec<u8>>::try_from(decimal)
        .map_err(|e| SchemaError(format!("Failed to convert decimal to bytes: {:?}", e)))?;

    // Convert bytes to BigInt
    let big_int = BigInt::from_signed_bytes_be(&bytes);

    // Create BigDecimal with the scale
    let big_decimal = BigDecimal::new(big_int, scale as i64);

    let mut decimal_str = big_decimal.to_plain_string();

    // In case a string like this: "1602189664.000000000000000000", drop trailing zeros
    if scale > 0
        && let Some(_) = decimal_str.find('.')
    {
        let trimmed = decimal_str.trim_end_matches('0');
        decimal_str = trimmed.trim_end_matches('.').to_string();
    }

    Ok(Some(decimal_str))
}

fn resolve_u8(v: &Value) -> AvroResult<u8> {
    let int = match v {
        Value::Int(n) => Ok(Value::Int(*n)),
        Value::Long(n) => Ok(Value::Int(*n as i32)),
        other => Err(AvroError::GetU8(other.into())),
    }?;
    if let Value::Int(n) = int
        && n >= 0
        && n <= From::from(u8::MAX)
    {
        return Ok(n as u8);
    }

    Err(AvroError::GetU8(int.into()))
}

fn resolve_bytes(v: &Value) -> Option<Vec<u8>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Bytes(_) => Ok(v.clone()),
        Value::String(s) => Ok(Value::Bytes(s.clone().into_bytes())),
        Value::Array(items) => Ok(Value::Bytes(
            items
                .iter()
                .map(resolve_u8)
                .collect::<Result<Vec<_>, _>>()
                .ok()?,
        )),
        other => Err(AvroError::GetBytes(other.into())),
    }
    .ok()
    .and_then(|v| match v {
        Value::Bytes(s) => Some(s),
        _ => None,
    })
}

fn resolve_fixed(v: &Value, size: usize) -> Option<Vec<u8>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Fixed(n, bytes) => {
            if *n == size {
                Some(bytes.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn resolve_boolean(value: &Value) -> Option<bool> {
    let v = if let Value::Union(_, b) = value {
        b
    } else {
        value
    };
    match v {
        Value::Boolean(boolean) => Some(*boolean),
        _ => None,
    }
}

fn resolve_decimal(v: &Value) -> ArrowResult<Option<i128>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Decimal(d) => {
            let value = <Vec<u8>>::try_from(d).unwrap();
            let bytes = value.to_bytes();

            // Check the sign bit of the first byte
            let negative = (bytes[0] & 0x80) != 0;

            // Create a 16-byte buffer for i128
            let mut extended = [0u8; 16];

            // Fill with sign bits
            let fill = if negative { 0xFF } else { 0x00 };

            // Fill the first part of the array with the sign extension bits
            extended[..16 - bytes.len()].fill(fill);

            // Copy Avro bytes at the end (least significant part)
            extended[(16 - bytes.len())..16].copy_from_slice(bytes);

            Ok(Some(i128::from_be_bytes(extended)))
        }
        Value::Null => Ok(None),
        other => Err(SchemaError(format!(
            "Expected Decimal value, got {:?}",
            other
        ))),
    }
}

/// Decode an Avro `Value::Decimal` into canonical decimal_arb bytes
/// (`[sign_byte][big-endian unsigned magnitude]` per
/// `contracts/arrow-extension-type.md` §3) at the column's declared scale.
fn resolve_decimal_arb_canonical_bytes(v: &Value, scale: u32) -> ArrowResult<Option<Vec<u8>>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Decimal(d) => {
            let bytes = <Vec<u8>>::try_from(d).map_err(|e| {
                SchemaError(format!("Failed to convert avro decimal to bytes: {:?}", e))
            })?;
            // Avro Decimal stores the value × 10^scale as signed two's-complement
            // big-endian bytes. Convert to BigInt, wrap as DecimalArbValue at the
            // column scale, then emit canonical bytes.
            let bigint = BigInt::from_signed_bytes_be(&bytes);
            let value = crate::types::decimal_arb::DecimalArbValue::from_bigint_and_scale(
                bigint,
                scale as i64,
            );
            Ok(Some(value.to_canonical_bytes_at_scale(scale)))
        }
        Value::Null => Ok(None),
        other => Err(SchemaError(format!(
            "Expected Decimal value for decimal_arb, got {:?}",
            other
        ))),
    }
}

fn resolve_u256(v: &Value) -> ArrowResult<Option<[u8; 32]>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Decimal(d) => {
            let value = <Vec<u8>>::try_from(d).unwrap();
            let bytes = value.to_bytes();

            // Check if negative (we don't support negative values for U256)
            if !bytes.is_empty() && (bytes[0] & 0x80) != 0 {
                return Err(SchemaError(
                    "Failed to convert negative decimal to U256 - negative values not supported"
                        .to_string(),
                ));
            }

            let mut bytes = bytes.to_vec();

            // Remove leading zero padding
            while bytes.len() > 32 && bytes[0] == 0x00 {
                bytes.remove(0);
            }

            // If still too large, cannot convert
            if bytes.len() > 32 {
                return Err(SchemaError(format!(
                    "Failed to convert decimal to U256 - data too large ({} bytes after padding removal, max 32)",
                    bytes.len()
                )));
            }

            // Pad to 32 bytes (big-endian)
            let mut result = [0u8; 32];
            let offset = 32 - bytes.len();
            result[offset..].copy_from_slice(&bytes);

            Ok(Some(result))
        }
        Value::Null => Ok(None),
        other => Err(SchemaError(format!(
            "Expected Decimal value for U256, got {:?}",
            other
        ))),
    }
}

fn resolve_i256(v: &Value) -> ArrowResult<Option<[u8; 32]>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Decimal(d) => {
            let value = <Vec<u8>>::try_from(d).unwrap();
            let bytes = value.to_bytes();
            let mut bytes = bytes.to_vec();
            let negative = !bytes.is_empty() && (bytes[0] & 0x80) != 0;

            // Remove padding bytes
            let padding_byte = if negative { 0xFF } else { 0x00 };
            while bytes.len() > 32 && bytes[0] == padding_byte {
                if bytes.len() > 1 {
                    let next_negative = (bytes[1] & 0x80) != 0;
                    if next_negative == negative {
                        bytes.remove(0);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            // If still too large, cannot convert
            if bytes.len() > 32 {
                return Err(SchemaError(format!(
                    "Failed to convert decimal to I256 - data too large ({} bytes after padding removal, max 32)",
                    bytes.len()
                )));
            }

            // Pad to 32 bytes (two's complement)
            let mut result = [padding_byte; 32];
            let offset = 32 - bytes.len();
            result[offset..].copy_from_slice(&bytes);

            Ok(Some(result))
        }
        Value::Null => Ok(None),
        other => Err(SchemaError(format!(
            "Expected Decimal value for I256, got {:?}",
            other
        ))),
    }
}

fn resolve_decimal_256(v: &Value) -> ArrowResult<Option<i256>> {
    // Note: We attempt conversion regardless of declared precision
    // For oversized data, we try to fit it by removing padding/sign extension
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Decimal(d) => {
            let value = <Vec<u8>>::try_from(d).unwrap();
            let mut bytes = value.to_bytes().to_vec(); // Ensure it's a Vec, not slice
            let original_size = bytes.len(); // Store original size before modification
            let negative = (bytes[0] & 0x80) != 0;

            // Data is larger than 32 bytes
            // remove padding and check try again.
            if bytes.len() > 32 {
                let padding_byte = if negative { 0xFF } else { 0x00 };

                // Remove leading padding bytes (0x00 for positive, 0xFF for negative)
                while bytes.len() > 32 && bytes[0] == padding_byte {
                    // Only remove if the next byte has the same sign bit
                    if bytes.len() > 1 {
                        let next_negative = (bytes[1] & 0x80) != 0;
                        if next_negative == negative {
                            bytes.remove(0);
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }

            // If still too large, return error to indicate conversion failure
            // We don't truncate more because that would change the actual value
            if bytes.len() > 32 {
                return Err(SchemaError(format!(
                    "Decimal data too large for Decimal256 after padding removal. \
                    Original size: {} bytes, after padding removal: {} bytes (max 32). \
                    Cannot convert without data loss.",
                    original_size,
                    bytes.len()
                )));
            }

            let mut extended = [0u8; 32];
            let fill = if negative { 0xFF } else { 0x00 };
            extended[..32 - bytes.len()].fill(fill);
            extended[(32 - bytes.len())..32].copy_from_slice(&bytes);
            Ok(Some(i256::from_be_bytes(extended)))
        }
        Value::Null => Ok(None),
        other => Err(SchemaError(format!(
            "Expected Decimal value, got {:?}",
            other
        ))),
    }
}

trait Resolver: ArrowPrimitiveType {
    fn resolve(value: &Value) -> Option<Self::Native>;
}

fn resolve_item<T: Resolver>(value: &Value) -> Option<T::Native> {
    T::resolve(value)
}

fn maybe_resolve_union(value: &Value) -> &Value {
    if SchemaKind::from(value) == SchemaKind::Union {
        // Pull out the Union, and attempt to resolve against it.
        match value {
            Value::Union(_, b) => b,
            _ => unreachable!(),
        }
    } else {
        value
    }
}

impl<N> Resolver for N
where
    N: ArrowNumericType,
    N::Native: NumCast,
{
    fn resolve(value: &Value) -> Option<Self::Native> {
        let value = maybe_resolve_union(value);
        match value {
            Value::Int(i) | Value::TimeMillis(i) | Value::Date(i) => NumCast::from(*i),
            Value::Long(l)
            | Value::TimeMicros(l)
            | Value::TimestampMillis(l)
            | Value::TimestampMicros(l) => NumCast::from(*l),
            Value::Float(f) => NumCast::from(*f),
            Value::Double(f) => NumCast::from(*f),
            Value::Duration(_d) => unimplemented!(), // shenanigans type
            Value::Null => None,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::i256::{bytes_to_i256, i256_to_string, string_to_i256};
    use crate::types::u256::{bytes_to_u256, string_to_u256, u256_to_string};
    use apache_avro::Decimal as AvroDecimal;
    use apache_avro::types::Value;

    #[test]
    fn test_resolve_u256_from_decimal() {
        // Test small positive value
        let decimal = AvroDecimal::from(vec![0x00, 0x00, 0x01, 0xFF]); // 511
        let value = Value::Decimal(decimal);
        let result = resolve_u256(&value).unwrap().unwrap();

        let u256_val = bytes_to_u256(&result);
        assert_eq!(u256_to_string(&u256_val), "511");
    }

    // ------- T018: decimal_arb resolver -------

    #[test]
    fn test_resolve_decimal_arb_positive() {
        // Avro decimal stores `value × 10^scale` as signed two's-complement.
        // value = 12345.678 at scale 3 → BigInt(12_345_678) → signed BE bytes.
        let bd: BigDecimal = "12345.678".parse().unwrap();
        let (bigint, _) = bd.into_bigint_and_exponent();
        let bytes = bigint.to_signed_bytes_be();
        let value = Value::Decimal(AvroDecimal::from(bytes));

        let canonical = resolve_decimal_arb_canonical_bytes(&value, 3)
            .unwrap()
            .unwrap();
        // Round-trip through DecimalArbValue at scale 3.
        let decoded = crate::types::decimal_arb::DecimalArbValue::from_canonical_bytes_at_scale(
            &canonical, 3,
        )
        .unwrap();
        assert_eq!(decoded.to_canonical_string(), "12345.678");
    }

    #[test]
    fn test_resolve_decimal_arb_negative() {
        let bd: BigDecimal = "-99.5".parse().unwrap();
        let (bigint, _) = bd.into_bigint_and_exponent();
        let bytes = bigint.to_signed_bytes_be();
        let value = Value::Decimal(AvroDecimal::from(bytes));

        let canonical = resolve_decimal_arb_canonical_bytes(&value, 1)
            .unwrap()
            .unwrap();
        let decoded = crate::types::decimal_arb::DecimalArbValue::from_canonical_bytes_at_scale(
            &canonical, 1,
        )
        .unwrap();
        assert_eq!(decoded.to_canonical_string(), "-99.5");
    }

    #[test]
    fn test_resolve_decimal_arb_100_digits() {
        // 100-digit integer with scale 0 — beyond Decimal256's reach.
        let mut s = String::with_capacity(101);
        s.push('1');
        for _ in 0..99 {
            s.push('0');
        }
        let bd: BigDecimal = s.parse().unwrap();
        let (bigint, _) = bd.into_bigint_and_exponent();
        let bytes = bigint.to_signed_bytes_be();
        let value = Value::Decimal(AvroDecimal::from(bytes));

        let canonical = resolve_decimal_arb_canonical_bytes(&value, 0)
            .unwrap()
            .unwrap();
        let decoded = crate::types::decimal_arb::DecimalArbValue::from_canonical_bytes_at_scale(
            &canonical, 0,
        )
        .unwrap();
        assert_eq!(decoded.to_canonical_string(), s);
    }

    #[test]
    fn test_resolve_decimal_arb_null() {
        assert!(
            resolve_decimal_arb_canonical_bytes(&Value::Null, 0)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_resolve_decimal_arb_with_union() {
        let bd: BigDecimal = "1.23".parse().unwrap();
        let (bigint, _) = bd.into_bigint_and_exponent();
        let bytes = bigint.to_signed_bytes_be();
        let value = Value::Union(0, Box::new(Value::Decimal(AvroDecimal::from(bytes))));

        let canonical = resolve_decimal_arb_canonical_bytes(&value, 2)
            .unwrap()
            .unwrap();
        let decoded = crate::types::decimal_arb::DecimalArbValue::from_canonical_bytes_at_scale(
            &canonical, 2,
        )
        .unwrap();
        assert_eq!(decoded.to_canonical_string(), "1.23");
    }

    #[test]
    fn test_resolve_u256_large_value() {
        // Test large U256 value
        let u256_val = string_to_u256("123456789012345678901234567890").unwrap();
        let bytes = crate::types::u256::u256_to_bytes(&u256_val);

        let decimal = AvroDecimal::from(bytes.to_vec());
        let value = Value::Decimal(decimal);
        let result = resolve_u256(&value).unwrap().unwrap();

        assert_eq!(result, bytes);
        let recovered = bytes_to_u256(&result);
        assert_eq!(u256_to_string(&recovered), "123456789012345678901234567890");
    }

    #[test]
    fn test_resolve_u256_large_boundary() {
        // Test a large U256 value (not quite max to avoid sign bit issues)
        // Using 2^255 - 1 (largest value without sign bit set)
        let mut large_bytes = [0xFFu8; 32];
        large_bytes[0] = 0x7F; // Clear sign bit

        let decimal = AvroDecimal::from(large_bytes.to_vec());
        let value = Value::Decimal(decimal);
        let result = resolve_u256(&value).unwrap().unwrap();

        assert_eq!(result, large_bytes);
    }

    #[test]
    fn test_resolve_u256_zero() {
        let decimal = AvroDecimal::from(vec![0x00]);
        let value = Value::Decimal(decimal);
        let result = resolve_u256(&value).unwrap().unwrap();

        let u256_val = bytes_to_u256(&result);
        assert_eq!(u256_to_string(&u256_val), "0");
    }

    #[test]
    fn test_resolve_u256_null() {
        let value = Value::Null;
        let result = resolve_u256(&value).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_u256_rejects_negative() {
        // Negative number in two's complement (sign bit set)
        let decimal = AvroDecimal::from(vec![0xFF, 0xFF, 0xFF, 0xFF]); // -1 in two's complement
        let value = Value::Decimal(decimal);
        let result = resolve_u256(&value);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("negative"));
    }

    #[test]
    fn test_resolve_u256_with_union() {
        // Test union variant
        let decimal = AvroDecimal::from(vec![0x00, 0x00, 0x01, 0x00]); // 256
        let value = Value::Union(0, Box::new(Value::Decimal(decimal)));
        let result = resolve_u256(&value).unwrap().unwrap();

        let u256_val = bytes_to_u256(&result);
        assert_eq!(u256_to_string(&u256_val), "256");
    }

    #[test]
    fn test_resolve_i256_from_decimal_positive() {
        // Test positive value
        let decimal = AvroDecimal::from(vec![0x00, 0x00, 0x01, 0xFF]); // 511
        let value = Value::Decimal(decimal);
        let result = resolve_i256(&value).unwrap().unwrap();

        let i256_val = bytes_to_i256(&result);
        assert_eq!(i256_to_string(&i256_val), "511");
    }

    #[test]
    fn test_resolve_i256_from_decimal_negative() {
        // Test negative value
        let i256_val = string_to_i256("-12345").unwrap();
        let bytes = crate::types::i256::i256_to_bytes(&i256_val);

        let decimal = AvroDecimal::from(bytes.to_vec());
        let value = Value::Decimal(decimal);
        let result = resolve_i256(&value).unwrap().unwrap();

        assert_eq!(result, bytes);
        let recovered = bytes_to_i256(&result);
        assert_eq!(i256_to_string(&recovered), "-12345");
    }

    #[test]
    fn test_resolve_i256_large_positive() {
        let i256_val = string_to_i256("123456789012345678901234567890").unwrap();
        let bytes = crate::types::i256::i256_to_bytes(&i256_val);

        let decimal = AvroDecimal::from(bytes.to_vec());
        let value = Value::Decimal(decimal);
        let result = resolve_i256(&value).unwrap().unwrap();

        assert_eq!(result, bytes);
        let recovered = bytes_to_i256(&result);
        assert_eq!(i256_to_string(&recovered), "123456789012345678901234567890");
    }

    #[test]
    fn test_resolve_i256_large_negative() {
        let i256_val = string_to_i256("-123456789012345678901234567890").unwrap();
        let bytes = crate::types::i256::i256_to_bytes(&i256_val);

        let decimal = AvroDecimal::from(bytes.to_vec());
        let value = Value::Decimal(decimal);
        let result = resolve_i256(&value).unwrap().unwrap();

        assert_eq!(result, bytes);
        let recovered = bytes_to_i256(&result);
        assert_eq!(
            i256_to_string(&recovered),
            "-123456789012345678901234567890"
        );
    }

    #[test]
    fn test_resolve_i256_zero() {
        let decimal = AvroDecimal::from(vec![0x00]);
        let value = Value::Decimal(decimal);
        let result = resolve_i256(&value).unwrap().unwrap();

        let i256_val = bytes_to_i256(&result);
        assert_eq!(i256_to_string(&i256_val), "0");
    }

    #[test]
    fn test_resolve_i256_negative_one() {
        let i256_val = string_to_i256("-1").unwrap();
        let bytes = crate::types::i256::i256_to_bytes(&i256_val);

        let decimal = AvroDecimal::from(bytes.to_vec());
        let value = Value::Decimal(decimal);
        let result = resolve_i256(&value).unwrap().unwrap();

        assert_eq!(result, bytes);
        let recovered = bytes_to_i256(&result);
        assert_eq!(i256_to_string(&recovered), "-1");
    }

    #[test]
    fn test_resolve_i256_null() {
        let value = Value::Null;
        let result = resolve_i256(&value).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_i256_with_union() {
        let i256_val = string_to_i256("-999").unwrap();
        let bytes = crate::types::i256::i256_to_bytes(&i256_val);

        let decimal = AvroDecimal::from(bytes.to_vec());
        let value = Value::Union(0, Box::new(Value::Decimal(decimal)));
        let result = resolve_i256(&value).unwrap().unwrap();

        assert_eq!(result, bytes);
        let recovered = bytes_to_i256(&result);
        assert_eq!(i256_to_string(&recovered), "-999");
    }

    #[test]
    fn test_resolve_i256_boundary_values() {
        // Test values near boundaries
        let test_values = vec!["0", "1", "-1", "127", "-128", "32767", "-32768"];

        for val_str in test_values {
            let i256_val = string_to_i256(val_str).unwrap();
            let bytes = crate::types::i256::i256_to_bytes(&i256_val);

            let decimal = AvroDecimal::from(bytes.to_vec());
            let value = Value::Decimal(decimal);
            let result = resolve_i256(&value).unwrap().unwrap();

            let recovered = bytes_to_i256(&result);
            assert_eq!(
                i256_to_string(&recovered),
                val_str,
                "Failed for value: {}",
                val_str
            );
        }
    }

    #[test]
    fn test_resolve_u256_boundary_values() {
        let test_values = vec!["0", "1", "255", "256", "65535", "65536"];

        for val_str in test_values {
            let u256_val = string_to_u256(val_str).unwrap();
            let bytes = crate::types::u256::u256_to_bytes(&u256_val);

            let decimal = AvroDecimal::from(bytes.to_vec());
            let value = Value::Decimal(decimal);
            let result = resolve_u256(&value).unwrap().unwrap();

            let recovered = bytes_to_u256(&result);
            assert_eq!(
                u256_to_string(&recovered),
                val_str,
                "Failed for value: {}",
                val_str
            );
        }
    }
}
