use crate::{
    DeliveryError,
    checked_queries::*,
    custody_record::{integer, stored_row_id},
    stored_outcome::*,
};
use fleet_sync_protocol::*;
use serde::de::DeserializeOwned;

pub(crate) trait PublicationRow {
    fn publication<A: DeserializeOwned>(&self) -> Result<StatusPublication<A>, DeliveryError>;
}
macro_rules! publication_row {
    ($($row:ty => $table:literal),+)=>{$(
        impl PublicationRow for $row {
            fn publication<A:DeserializeOwned>(&self)->Result<StatusPublication<A>,DeliveryError> {
                let key=format!("{}/{}/{}",self.submitter_id.as_deref().unwrap_or("<missing>"),self.item_id.as_deref().unwrap_or("<missing>"),self.version.as_deref().unwrap_or("<missing>"));
                stored_row_id(&self.id,$table,key)?;
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
    StoredStatusEventRow => "_fleet_sync_status_events",
    StoredPublicationRow => "_fleet_sync_status_owed"
);

impl StoredPublicationRow {
    pub(crate) fn target<R: DeserializeOwned + serde::Serialize>(
        &self,
    ) -> Result<crate::PublicationTarget<R>, DeliveryError> {
        stored_publication_target(
            self.target_kind.clone(),
            self.submitter_kind.clone(),
            self.submitter_id.clone(),
            self.status_route.clone(),
        )
    }
}

pub(crate) fn stored_publication_target<R: DeserializeOwned + serde::Serialize>(
    target_kind: Option<String>,
    submitter_kind: Option<String>,
    submitter_id: Option<String>,
    status_route: Option<String>,
) -> Result<crate::PublicationTarget<R>, DeliveryError> {
    let owner = crate::submitter::from_columns(submitter_kind, submitter_id)?.ok_or(
        DeliveryError::Corrupt {
            field: "publication submitter",
        },
    )?;
    let route = crate::custody_record::decode(status_route.clone(), "publication route")?;
    if status_route.as_deref() != Some(crate::status_stream::canonical_encode(&route)?.as_str()) {
        return Err(DeliveryError::Corrupt {
            field: "publication route encoding",
        });
    }
    match (target_kind.as_deref(), owner) {
        (Some("statusStream"), sender) => {
            Ok(crate::PublicationTarget::StatusStream { sender, route })
        }
        (Some("peer"), crate::Submitter::Machine(peer)) => {
            Ok(crate::PublicationTarget::Peer { peer, route })
        }
        (Some("project"), crate::Submitter::Project(project)) => {
            Ok(crate::PublicationTarget::Project { project, route })
        }
        _ => Err(DeliveryError::Corrupt {
            field: "publication target kind",
        }),
    }
}
