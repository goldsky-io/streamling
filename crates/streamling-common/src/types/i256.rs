#![allow(clippy::manual_div_ceil)]
use crate::error::ResultExt;
use crate::{streamling_err, streamling_user_err};
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;
use uint::construct_uint;

construct_uint! {
    /// Internal 256-bit unsigned integer used for I256 representation
    pub struct U256(4);  // 4 × 64-bit = 256 bits
}

/// Extension type for signed 256-bit integers
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct I256Type;

impl I256Type {
    pub const EXTENSION_NAME: &'static str = "streamling.i256";
    pub const METADATA_KEY: &'static str = "ARROW:extension:name";

    /// Create a new I256 type (FixedSizeBinary(32) with metadata)
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> DataType {
        DataType::FixedSizeBinary(32)
    }

    /// Create field metadata for I256
    pub fn metadata() -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            Self::METADATA_KEY.to_string(),
            Self::EXTENSION_NAME.to_string(),
        );
        metadata
    }

    /// Check if a field's metadata indicates I256 type
    pub fn is_i256_metadata(metadata: &HashMap<String, String>) -> bool {
        metadata
            .get(Self::METADATA_KEY)
            .map(|v| v == Self::EXTENSION_NAME)
            .unwrap_or(false)
    }

    /// Check if a Field is an I256 type (FixedSizeBinary(32) with I256 metadata)
    pub fn is_i256_field(field: &Field) -> bool {
        matches!(field.data_type(), DataType::FixedSizeBinary(32))
            && Self::is_i256_metadata(field.metadata())
    }
}

impl Default for I256Type {
    fn default() -> Self {
        Self
    }
}

/// Represents a signed 256-bit integer using two's complement
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct I256 {
    value: U256,
}

impl I256 {
    /// Create I256 from U256 (two's complement representation)
    pub fn from_u256(value: U256) -> Self {
        Self { value }
    }

    /// Get the underlying U256 value (two's complement)
    pub fn to_u256(&self) -> U256 {
        self.value
    }

    /// Create I256 from i64
    pub fn from_i64(n: i64) -> Self {
        if n >= 0 {
            Self {
                value: U256::from(n as u64),
            }
        } else {
            // Two's complement: negate and subtract 1, then bitwise not
            let abs_val = U256::from(n.unsigned_abs());
            let value = !abs_val + U256::from(1u64);
            Self { value }
        }
    }

    /// Check if the value is negative (sign bit is set)
    pub fn is_negative(&self) -> bool {
        // Check if the most significant bit is set
        self.value.bit(255)
    }

    /// Check if the value is zero
    pub fn is_zero(&self) -> bool {
        self.value.is_zero()
    }

    /// Negate the value
    pub fn neg(&self) -> Self {
        Self {
            value: !self.value + U256::from(1u64),
        }
    }

    /// Absolute value
    pub fn abs(&self) -> Self {
        if self.is_negative() {
            self.neg()
        } else {
            *self
        }
    }

    /// Add two I256 values
    pub fn checked_add(&self, other: &Self) -> Option<Self> {
        let result = self.value.overflowing_add(other.value).0;
        let result_i256 = Self { value: result };

        // Check for overflow: if both operands have same sign and result has different sign
        let self_neg = self.is_negative();
        let other_neg = other.is_negative();
        let result_neg = result_i256.is_negative();

        if self_neg == other_neg && self_neg != result_neg {
            None // Overflow occurred
        } else {
            Some(result_i256)
        }
    }

    /// Subtract two I256 values
    pub fn checked_sub(&self, other: &Self) -> Option<Self> {
        let result = self.value.overflowing_sub(other.value).0;
        let result_i256 = Self { value: result };

        // Check for overflow
        let self_neg = self.is_negative();
        let other_neg = other.is_negative();
        let result_neg = result_i256.is_negative();

        if self_neg != other_neg && self_neg != result_neg {
            None // Overflow occurred
        } else {
            Some(result_i256)
        }
    }

    /// Multiply two I256 values
    pub fn checked_mul(&self, other: &Self) -> Option<Self> {
        // For simplicity, convert to absolute values, multiply, then apply sign
        let self_neg = self.is_negative();
        let other_neg = other.is_negative();
        let result_should_be_neg = self_neg != other_neg;

        let abs_self = if self_neg {
            self.neg().value
        } else {
            self.value
        };
        let abs_other = if other_neg {
            other.neg().value
        } else {
            other.value
        };

        let (result, overflow) = abs_self.overflowing_mul(abs_other);

        if overflow {
            return None;
        }

        // Check if result fits in I256 range
        // Max positive I256 = 2^255 - 1
        // Max negative I256 = 2^255
        let max_positive = U256::from(1u64) << 255;
        if result >= max_positive {
            if result_should_be_neg && result == max_positive {
                // Special case: -2^255 is valid
                return Some(Self { value: result });
            }
            return None;
        }

        let result = if result_should_be_neg {
            Self { value: result }.neg()
        } else {
            Self { value: result }
        };

        Some(result)
    }

    /// Divide two I256 values
    pub fn checked_div(&self, other: &Self) -> Option<Self> {
        if other.is_zero() {
            return None;
        }

        let self_neg = self.is_negative();
        let other_neg = other.is_negative();
        let result_should_be_neg = self_neg != other_neg;

        let abs_self = if self_neg {
            self.neg().value
        } else {
            self.value
        };
        let abs_other = if other_neg {
            other.neg().value
        } else {
            other.value
        };

        let result = abs_self / abs_other;
        let result = if result_should_be_neg {
            Self { value: result }.neg()
        } else {
            Self { value: result }
        };

        Some(result)
    }

    /// Modulo operation
    pub fn checked_rem(&self, other: &Self) -> Option<Self> {
        if other.is_zero() {
            return None;
        }

        let self_neg = self.is_negative();
        let abs_self = if self_neg {
            self.neg().value
        } else {
            self.value
        };
        let abs_other = if other.is_negative() {
            other.neg().value
        } else {
            other.value
        };

        let result = abs_self % abs_other;
        let result = if self_neg {
            Self { value: result }.neg()
        } else {
            Self { value: result }
        };

        Some(result)
    }
}

/// Convert bytes to I256
pub fn bytes_to_i256(bytes: &[u8; 32]) -> I256 {
    let value = U256::from_big_endian(bytes);
    I256::from_u256(value)
}

/// Convert I256 to bytes
pub fn i256_to_bytes(value: &I256) -> [u8; 32] {
    value.to_u256().to_big_endian()
}

/// Parse decimal string to I256
pub fn string_to_i256(s: &str) -> crate::error::Result<I256> {
    let s = s.trim();
    if let Some(abs_str) = s.strip_prefix('-') {
        let abs_val = U256::from_dec_str(abs_str)
            .map_err(|e| streamling_user_err!("{}", e))
            .streamling_with_context(|| format!("failed to parse I256 from string '{}'", s))?;
        Ok(I256::from_u256(abs_val).neg())
    } else {
        let val = U256::from_dec_str(s)
            .map_err(|e| streamling_user_err!("{}", e))
            .streamling_with_context(|| format!("failed to parse I256 from string '{}'", s))?;
        Ok(I256::from_u256(val))
    }
}

/// Convert I256 to decimal string
pub fn i256_to_string(value: &I256) -> String {
    if value.is_negative() {
        format!("-{}", value.neg().to_u256())
    } else {
        value.to_u256().to_string()
    }
}

/// Add two I256 values with overflow checking
pub fn add(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_i256(a);
    let b_val = bytes_to_i256(b);
    let result = a_val
        .checked_add(&b_val)
        .ok_or_else(|| streamling_err!("I256 addition overflow"))?;
    Ok(i256_to_bytes(&result))
}

/// Subtract two I256 values with overflow checking
pub fn sub(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_i256(a);
    let b_val = bytes_to_i256(b);
    let result = a_val
        .checked_sub(&b_val)
        .ok_or_else(|| streamling_err!("I256 subtraction overflow"))?;
    Ok(i256_to_bytes(&result))
}

/// Multiply two I256 values with overflow checking
pub fn mul(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_i256(a);
    let b_val = bytes_to_i256(b);
    let result = a_val
        .checked_mul(&b_val)
        .ok_or_else(|| streamling_err!("I256 multiplication overflow"))?;
    Ok(i256_to_bytes(&result))
}

/// Divide two I256 values
pub fn div(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_i256(a);
    let b_val = bytes_to_i256(b);
    let result = a_val
        .checked_div(&b_val)
        .ok_or_else(|| streamling_err!("I256 division by zero"))?;
    Ok(i256_to_bytes(&result))
}

/// Modulo operation on two I256 values
pub fn rem(a: &[u8; 32], b: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_i256(a);
    let b_val = bytes_to_i256(b);
    let result = a_val
        .checked_rem(&b_val)
        .ok_or_else(|| streamling_err!("I256 modulo by zero"))?;
    Ok(i256_to_bytes(&result))
}

/// Negate an I256 value
pub fn neg(a: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_i256(a);
    let result = a_val.neg();
    Ok(i256_to_bytes(&result))
}

/// Absolute value of an I256
pub fn abs(a: &[u8; 32]) -> crate::error::Result<[u8; 32]> {
    let a_val = bytes_to_i256(a);
    let result = a_val.abs();
    Ok(i256_to_bytes(&result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i256_positive() {
        let value = I256::from_i64(12345);
        let bytes = i256_to_bytes(&value);
        let restored = bytes_to_i256(&bytes);
        assert_eq!(value, restored);
    }

    #[test]
    fn test_i256_negative() {
        let value = I256::from_i64(-12345);
        let bytes = i256_to_bytes(&value);
        let restored = bytes_to_i256(&bytes);
        assert_eq!(value, restored);
        assert!(restored.is_negative());
    }

    #[test]
    fn test_i256_string_conversion() {
        let value = I256::from_i64(-12345);
        let string = i256_to_string(&value);
        assert_eq!(string, "-12345");
        let restored = string_to_i256(&string).unwrap();
        assert_eq!(value, restored);
    }

    #[test]
    fn test_i256_add() {
        let a = i256_to_bytes(&I256::from_i64(100));
        let b = i256_to_bytes(&I256::from_i64(-50));
        let result = add(&a, &b).unwrap();
        let result_val = bytes_to_i256(&result);
        assert_eq!(result_val, I256::from_i64(50));
    }

    #[test]
    fn test_i256_divide_by_zero_error() {
        let a = i256_to_bytes(&I256::from_i64(42));
        let zero = i256_to_bytes(&I256::from_i64(0));
        let err = div(&a, &zero).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("division by zero") || msg.contains("division"));
    }

    #[test]
    fn test_i256_modulo_by_zero_error() {
        let a = i256_to_bytes(&I256::from_i64(42));
        let zero = i256_to_bytes(&I256::from_i64(0));
        let err = rem(&a, &zero).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("modulo by zero") || msg.contains("modulo"));
    }

    #[test]
    fn test_i256_min_divide_negative_one_wraps_current_behavior() {
        // Construct INT256_MIN = -2^255
        let min = I256::from_u256(U256::from(1u64) << 255);
        let neg_one = I256::from_i64(-1);
        let min_bytes = i256_to_bytes(&min);
        let neg_one_bytes = i256_to_bytes(&neg_one);
        // Current implementation wraps; document it here
        let result = div(&min_bytes, &neg_one_bytes).unwrap();
        let val = bytes_to_i256(&result);
        assert_eq!(val, min);
    }
}
