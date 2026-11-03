use crate::{DeliveryError, checked_queries::*, custody_record::integer, stored_outcome::*};
use fleet_sync_protocol::*;
use serde::de::DeserializeOwned;

pub(crate) trait PublicationRow {
    fn publication<A: DeserializeOwned>(&self) -> Result<StatusPublication<A>, DeliveryError>;
}
macro_rules! publication_row {
    ($($row:ty),+)=>{$(
        impl PublicationRow for $row {
            fn publication<A:DeserializeOwned>(&self)->Result<StatusPublication<A>,DeliveryError> {
                Ok(StatusPublication {
                    item:stored_enum(self.item_id.clone(),"status item id")?,
                    version:StatusVersion::new(integer(self.version.clone(),self.version_class.clone(),"status version")?),
                    status:stored_status(self.status_kind.clone(),None,self.status_acceptance.clone(),RefusalFields {
                        kind:self.refusal_kind.clone(),code:self.refusal_code.clone(),failure:self.refusal_failure.clone(),size_field:self.refusal_size_field.clone(),digest:self.refusal_digest.clone(),item_id:self.refusal_item_id.clone(),
                    })?,
                    cancel:stored_cancel(self.cancel_outcome.clone(),self.cancel_too_late_at.clone())?,
                })
            }
        }
    )+};
}
publication_row!(
    StoredStatusEventRow,
    StoredPublicationRow,
    StoredOwedPublicationRow
);
