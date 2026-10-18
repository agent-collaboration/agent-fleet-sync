//! Retain Serde's reported field without interpreting its Display diagnostic.
use fleet_sync_protocol::{ClientFrame, ConsumerProtocol};
use serde::{
    Deserialize,
    de::{
        self, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess,
        Visitor,
    },
};
use std::{cell::Cell, fmt};

pub(crate) fn invalid_shape_field<Protocol: ConsumerProtocol>(frame: &[u8]) -> String {
    let mut decoder = serde_json::Deserializer::from_slice(frame);
    ClientFrame::<Protocol>::deserialize(FieldDeserializer(&mut decoder))
        .err()
        .and_then(|error| error.field)
        .unwrap_or_else(|| "body".into())
        .chars()
        .take(64)
        .collect()
}

#[derive(Debug)]
struct FieldError {
    field: Option<String>,
}
impl fmt::Display for FieldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid session frame")
    }
}
impl std::error::Error for FieldError {}
impl de::Error for FieldError {
    fn custom<Message: fmt::Display>(_message: Message) -> Self {
        Self { field: None }
    }
    fn unknown_field(field: &str, _expected: &'static [&'static str]) -> Self {
        Self {
            field: Some(field.into()),
        }
    }
    fn missing_field(field: &'static str) -> Self {
        Self {
            field: Some(field.into()),
        }
    }
    fn duplicate_field(field: &'static str) -> Self {
        Self {
            field: Some(field.into()),
        }
    }
}

// A delegated Serde call must abort its underlying parser on error. Carry the
// returned structured error through that one call, then restore it immediately.
// No capture survives a call that a consumer catches (for example an untagged
// variant attempt), so discarded errors cannot become the final field.
type ErrorSlot = Cell<Option<FieldError>>;
fn record_error<Value, Error: de::Error>(
    result: Result<Value, FieldError>,
    slot: &ErrorSlot,
) -> Result<Value, Error> {
    result.map_err(|error| {
        slot.set(Some(error));
        Error::custom("invalid session frame")
    })
}
fn restore_error<Value, Error: fmt::Display>(
    result: Result<Value, Error>,
    slot: &ErrorSlot,
) -> Result<Value, FieldError> {
    result.map_err(|_| slot.take().unwrap_or(FieldError { field: None }))
}

struct FieldDeserializer<Source>(Source);
macro_rules! delegate_deserializers {
    ($($method:ident $(, $($parameter:ident: $parameter_type:ty),+)?;)*) => {
        $(fn $method<Target>(self, $($($parameter: $parameter_type,)*)? visitor: Target) -> Result<Target::Value, Self::Error>
        where Target: Visitor<'de> {
            let error = ErrorSlot::new(None);
            let result = self.0.$method($($($parameter,)*)? FieldVisitor { visitor, error: &error });
            restore_error(result, &error)
        })*
    };
}
impl<'de, Source: Deserializer<'de>> Deserializer<'de> for FieldDeserializer<Source> {
    type Error = FieldError;
    delegate_deserializers! {
        deserialize_any; deserialize_bool;
        deserialize_i8; deserialize_i16; deserialize_i32; deserialize_i64; deserialize_i128;
        deserialize_u8; deserialize_u16; deserialize_u32; deserialize_u64; deserialize_u128;
        deserialize_f32; deserialize_f64; deserialize_char; deserialize_str; deserialize_string;
        deserialize_bytes; deserialize_byte_buf; deserialize_option; deserialize_unit;
        deserialize_unit_struct, name: &'static str;
        deserialize_newtype_struct, name: &'static str;
        deserialize_seq; deserialize_tuple, length: usize;
        deserialize_tuple_struct, name: &'static str, length: usize;
        deserialize_map;
        deserialize_struct, name: &'static str, fields: &'static [&'static str];
        deserialize_enum, name: &'static str, variants: &'static [&'static str];
        deserialize_identifier; deserialize_ignored_any;
    }
    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

struct FieldVisitor<'capture, Target> {
    visitor: Target,
    error: &'capture ErrorSlot,
}
macro_rules! delegate_visits {
    ($($method:ident $(, $parameter:ident: $parameter_type:ty)?;)*) => {
        $(fn $method<Error: de::Error>(self, $($parameter: $parameter_type)?) -> Result<Self::Value, Error> {
            record_error(self.visitor.$method::<FieldError>($($parameter)?), self.error)
        })*
    };
}
impl<'de, Target: Visitor<'de>> Visitor<'de> for FieldVisitor<'_, Target> {
    type Value = Target::Value;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.visitor.expecting(formatter)
    }
    delegate_visits! {
        visit_bool, value: bool;
        visit_i8, value: i8; visit_i16, value: i16; visit_i32, value: i32; visit_i64, value: i64; visit_i128, value: i128;
        visit_u8, value: u8; visit_u16, value: u16; visit_u32, value: u32; visit_u64, value: u64; visit_u128, value: u128;
        visit_f32, value: f32; visit_f64, value: f64; visit_char, value: char;
        visit_str, value: &str; visit_borrowed_str, value: &'de str; visit_string, value: String;
        visit_bytes, value: &[u8]; visit_borrowed_bytes, value: &'de [u8]; visit_byte_buf, value: Vec<u8>;
        visit_none; visit_unit;
    }
    fn visit_some<Source: Deserializer<'de>>(
        self,
        decoder: Source,
    ) -> Result<Self::Value, Source::Error> {
        record_error(
            self.visitor.visit_some(FieldDeserializer(decoder)),
            self.error,
        )
    }
    fn visit_newtype_struct<Source: Deserializer<'de>>(
        self,
        decoder: Source,
    ) -> Result<Self::Value, Source::Error> {
        record_error(
            self.visitor
                .visit_newtype_struct(FieldDeserializer(decoder)),
            self.error,
        )
    }
    fn visit_seq<Source: SeqAccess<'de>>(
        self,
        access: Source,
    ) -> Result<Self::Value, Source::Error> {
        record_error(self.visitor.visit_seq(FieldSequence(access)), self.error)
    }
    fn visit_map<Source: MapAccess<'de>>(
        self,
        access: Source,
    ) -> Result<Self::Value, Source::Error> {
        record_error(self.visitor.visit_map(FieldMap(access)), self.error)
    }
    fn visit_enum<Source: EnumAccess<'de>>(
        self,
        access: Source,
    ) -> Result<Self::Value, Source::Error> {
        record_error(self.visitor.visit_enum(FieldEnum(access)), self.error)
    }
}

struct FieldSeed<'capture, Target> {
    seed: Target,
    error: &'capture ErrorSlot,
}
impl<'de, Target: DeserializeSeed<'de>> DeserializeSeed<'de> for FieldSeed<'_, Target> {
    type Value = Target::Value;
    fn deserialize<Source: Deserializer<'de>>(
        self,
        decoder: Source,
    ) -> Result<Self::Value, Source::Error> {
        record_error(
            self.seed.deserialize(FieldDeserializer(decoder)),
            self.error,
        )
    }
}
struct FieldSequence<Source>(Source);
impl<'de, Source: SeqAccess<'de>> SeqAccess<'de> for FieldSequence<Source> {
    type Error = FieldError;
    fn next_element_seed<Target: DeserializeSeed<'de>>(
        &mut self,
        seed: Target,
    ) -> Result<Option<Target::Value>, Self::Error> {
        let error = ErrorSlot::new(None);
        let result = self.0.next_element_seed(FieldSeed {
            seed,
            error: &error,
        });
        restore_error(result, &error)
    }
    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}
struct FieldMap<Source>(Source);
impl<'de, Source: MapAccess<'de>> MapAccess<'de> for FieldMap<Source> {
    type Error = FieldError;
    fn next_key_seed<Target: DeserializeSeed<'de>>(
        &mut self,
        seed: Target,
    ) -> Result<Option<Target::Value>, Self::Error> {
        let error = ErrorSlot::new(None);
        let result = self.0.next_key_seed(FieldSeed {
            seed,
            error: &error,
        });
        restore_error(result, &error)
    }
    fn next_value_seed<Target: DeserializeSeed<'de>>(
        &mut self,
        seed: Target,
    ) -> Result<Target::Value, Self::Error> {
        let error = ErrorSlot::new(None);
        let result = self.0.next_value_seed(FieldSeed {
            seed,
            error: &error,
        });
        restore_error(result, &error)
    }
    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}
struct FieldEnum<Source>(Source);
impl<'de, Source: EnumAccess<'de>> EnumAccess<'de> for FieldEnum<Source> {
    type Error = FieldError;
    type Variant = FieldVariant<Source::Variant>;
    fn variant_seed<Target: DeserializeSeed<'de>>(
        self,
        seed: Target,
    ) -> Result<(Target::Value, Self::Variant), Self::Error> {
        let error = ErrorSlot::new(None);
        let result = self.0.variant_seed(FieldSeed {
            seed,
            error: &error,
        });
        restore_error(result, &error).map(|(value, variant)| (value, FieldVariant(variant)))
    }
}
struct FieldVariant<Source>(Source);
impl<'de, Source: VariantAccess<'de>> VariantAccess<'de> for FieldVariant<Source> {
    type Error = FieldError;
    fn unit_variant(self) -> Result<(), Self::Error> {
        self.0
            .unit_variant()
            .map_err(|_| FieldError { field: None })
    }
    fn newtype_variant_seed<Target: DeserializeSeed<'de>>(
        self,
        seed: Target,
    ) -> Result<Target::Value, Self::Error> {
        let error = ErrorSlot::new(None);
        let result = self.0.newtype_variant_seed(FieldSeed {
            seed,
            error: &error,
        });
        restore_error(result, &error)
    }
    fn tuple_variant<Target: Visitor<'de>>(
        self,
        length: usize,
        visitor: Target,
    ) -> Result<Target::Value, Self::Error> {
        let error = ErrorSlot::new(None);
        let result = self.0.tuple_variant(
            length,
            FieldVisitor {
                visitor,
                error: &error,
            },
        );
        restore_error(result, &error)
    }
    fn struct_variant<Target: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: Target,
    ) -> Result<Target::Value, Self::Error> {
        let error = ErrorSlot::new(None);
        let result = self.0.struct_variant(
            fields,
            FieldVisitor {
                visitor,
                error: &error,
            },
        );
        restore_error(result, &error)
    }
}
