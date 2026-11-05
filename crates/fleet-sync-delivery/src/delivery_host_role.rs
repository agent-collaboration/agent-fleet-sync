/// The host declares its delivery storage role when constructing its lifecycle.
/// Relay custody never owns a recipient Inbox or a machine's status cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryHostRole {
    Machine,
    Relay,
}
