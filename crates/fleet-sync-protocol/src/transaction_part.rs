use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{CommitSequence, Epoch, PieceDigest};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransactionKey {
    pub epoch: Epoch,
    pub commit_id: CommitSequence,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TransactionPartError {
    #[error("transaction part count must be greater than zero")]
    Empty,
    #[error("transaction part index must be below its count")]
    IndexOutOfBounds,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionPart {
    pub transaction: TransactionKey,
    pub index: u32,
    pub count: u32,
    pub transaction_digest: PieceDigest,
}

impl TransactionPart {
    pub fn new(
        transaction: TransactionKey,
        index: u32,
        count: u32,
        transaction_digest: PieceDigest,
    ) -> Result<Self, TransactionPartError> {
        if count == 0 {
            return Err(TransactionPartError::Empty);
        }
        if index >= count {
            return Err(TransactionPartError::IndexOutOfBounds);
        }
        Ok(Self {
            transaction,
            index,
            count,
            transaction_digest,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawTransactionPart {
    transaction: TransactionKey,
    index: u32,
    count: u32,
    transaction_digest: PieceDigest,
}

impl<'de> Deserialize<'de> for TransactionPart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawTransactionPart::deserialize(deserializer)?;
        Self::new(
            raw.transaction,
            raw.index,
            raw.count,
            raw.transaction_digest,
        )
        .map_err(D::Error::custom)
    }
}
