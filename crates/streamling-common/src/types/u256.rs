#![allow(clippy::manual_div_ceil)]
use crate::error::ResultExt;
use crate::{streamling_err, streamling_user_err};
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;
use uint::construct_uint;

construct_uint! {
    /// 256-bit unsigned integer
    pub struct U256(4);  // 4 × 64-bit = 256 bits
}

/// Extension type for unsigned 256-bit integers
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct U256Type;

impl U256Type {
    pub const EXTENSION_NAME: &'static str = "streamling.u256";
    pub const METADATA_KEY: &'static str = "ARROW:extension:name";

    /// Create a new U256 type (FixedSizeBinary(32) with metadata)
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> DataType {
        DataType::FixedSizeBinary(32)
    }

    /// Create field metadata for U256
    pub fn metadata() -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            Self::METADATA_KEY.to_string(),
            Self::EXTENSION_NAME.to_string(),
        );
        metadata
    }

    /// Check if a field's metadata indicates U256 type
    pub fn is_u256_metadata(metadata: &HashMap<String, String>) -> bool {
        metadata
            .get(Self::METADATA_KEY)
            .map(|v| v == Self::EXTENSION_NAME)
            .unwrap_or(false)
    }

    /// Check if a Field is a U256 type (FixedSizeBinary(32) with U256 metadata)
    pub fn is_u256_field(field: &Field) -> bool {
        matches!(field.data_type(), DataType::FixedSizeBinary(32))
            && Self::is_u256_metadata(field.metadata())
    }
}

impl Default for U256Type {
    fn default() -> Self {
        Self
    }
}

/// Convert bytes to U256
pub fn bytes_to_u256(bytes: &[u8; 32]) -> U256 {
    U256::from_big_endian(bytes)
}

/// Convert U256 to bytes
pub fn u256_to_bytes(value: &U256) -> [u8; 32] {
    value.to_big_endian()
}

/// Parse decimal string to U256
pub fn string_to_u256(s: &str) -> crate::error::Result<U256> {
    U256::from_dec_str(s)
        .map_err(|e| streamling_user_err!("{}", e))
        .streamling_with_context(|| format!("failed to parse U256 from string '{}'", s))
}

/// Convert U256 to decimal string
pub fn u256_to_string(value: &U256) -> String {
    value.to_string()
}

/// Parse hex string to U256 (with or without 0x prefix)
pub fn hex_string_to_u256(s: &str) -> crate::error::Result<U256> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    U256::from_str_radix(s, 16)
        .map_err(|e| streamling_user_err!("{}", e))
        .streamling_with_context(|| format!("failed to parse U256 from hex '{}'", s))
}

/// Convert U256 to hex string with 0x prefix
pub fn u256_to_hex_string(value: &U256) -> String {
    format!("0x{:x}", value)
}

/// Add two U256 values with overflow checking
pub fn add(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_u256(a);
    let b_val = bytes_to_u256(b);
    let result = a_val
        .checked_add(b_val)
        .ok_or_else(|| streamling_err!("U256 addition overflow"))?;
    Ok(u256_to_bytes(&result))
}

/// Subtract two U256 values with underflow checking
pub fn sub(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_u256(a);
    let b_val = bytes_to_u256(b);
    let result = a_val
        .checked_sub(b_val)
        .ok_or_else(|| streamling_err!("U256 subtraction underflow"))?;
    Ok(u256_to_bytes(&result))
}

/// Multiply two U256 values with overflow checking
pub fn mul(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_u256(a);
    let b_val = bytes_to_u256(b);
    let result = a_val
        .checked_mul(b_val)
        .ok_or_else(|| streamling_err!("U256 multiplication overflow"))?;
    Ok(u256_to_bytes(&result))
}

/// Divide two U256 values
pub fn div(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_u256(a);
    let b_val = bytes_to_u256(b);
    if b_val.is_zero() {
        return Err(streamling_err!("U256 division by zero"));
    }
    let result = a_val / b_val;
    Ok(u256_to_bytes(&result))
}

/// Modulo operation on two U256 values
pub fn rem(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_u256(a);
    let b_val = bytes_to_u256(b);
    if b_val.is_zero() {
        return Err(streamling_err!("U256 modulo by zero"));
    }
    let result = a_val % b_val;
    Ok(u256_to_bytes(&result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u256_conversion() {
        let value = U256::from(12345u64);
        let bytes = u256_to_bytes(&value);
        let restored = bytes_to_u256(&bytes);
        assert!(U256Type::is_u256_field(
            &Field::new("u", U256Type::new(), false).with_metadata(U256Type::metadata())
        ));
        assert_eq!(value, restored);
    }

    #[test]
    fn test_u256_string_conversion() {
        let value = U256::from(12345u64);
        let string = u256_to_string(&value);
        assert_eq!(string, "12345");
        let restored = string_to_u256(&string).unwrap();
        assert_eq!(value, restored);
    }

    #[test]
    fn test_u256_add() {
        let a = u256_to_bytes(&U256::from(100u64));
        let b = u256_to_bytes(&U256::from(50u64));
        let result = add(&a, &b).unwrap();
        let result_val = bytes_to_u256(&result);
        assert_eq!(result_val, U256::from(150u64));
    }

    #[test]
    fn test_u256_overflow() {
        let max = u256_to_bytes(&U256::max_value());
        let one = u256_to_bytes(&U256::from(1u64));
        assert!(add(&max, &one).is_err());
    }

    #[test]
    fn test_u256_divide_by_zero_error() {
        let a = u256_to_bytes(&U256::from(10u64));
        let zero = u256_to_bytes(&U256::from(0u64));
        let err = div(&a, &zero).unwrap_err();
        assert!(format!("{}", err).to_lowercase().contains("zero"));
    }

    #[test]
    fn test_u256_modulo_by_zero_error() {
        let a = u256_to_bytes(&U256::from(10u64));
        let zero = u256_to_bytes(&U256::from(0u64));
        let err = rem(&a, &zero).unwrap_err();
        assert!(format!("{}", err).to_lowercase().contains("zero"));
    }
}
