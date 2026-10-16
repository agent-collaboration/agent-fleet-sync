use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ReplicatedTransaction;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PieceDigest(String);

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DigestError {
    #[error("digest must be 64 lowercase hexadecimal characters")]
    InvalidDigest,
    #[error("cannot encode replication content: {0}")]
    Encoding(String),
}

impl TryFrom<String> for PieceDigest {
    type Error = DigestError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DigestError::InvalidDigest);
        }
        Ok(Self(value))
    }
}

impl From<PieceDigest> for String {
    fn from(value: PieceDigest) -> Self {
        value.0
    }
}

impl PieceDigest {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn of_transaction(transaction: &ReplicatedTransaction) -> Result<Self, DigestError> {
        Self::of_canonical(transaction)
    }

    // The encoding uses only closed, typed structs/enums and ordered sequences.
    // serde declaration order fixes object fields; arbitrary JSON maps never enter
    // a digest. Integer SQL values stay exact, including values beyond 2^53.
    pub(crate) fn of_canonical<TContent: Serialize>(
        content: &TContent,
    ) -> Result<Self, DigestError> {
        let encoded = serde_json::to_vec(content)
            .map_err(|error| DigestError::Encoding(error.to_string()))?;
        let hash = Sha256::digest(encoded);
        let mut hexadecimal = String::with_capacity(64);
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        for byte in hash {
            for nibble in [byte >> 4, byte & 15] {
                if let Some(character) = DIGITS.get(usize::from(nibble)) {
                    hexadecimal.push(char::from(*character));
                }
            }
        }
        Ok(Self(hexadecimal))
    }
}
