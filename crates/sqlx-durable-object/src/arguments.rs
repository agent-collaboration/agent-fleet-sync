use sqlx_core::arguments::Arguments;
use sqlx_core::encode::Encode;
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use sqlx_durable_object_core::add_argument;

use crate::{Do, DoArgumentValue};

/// The parameters bound to one query, in placeholder order.
///
/// `format_placeholder` keeps its default, `?`, which is what Durable Object storage expects.
#[derive(Debug, Default, Clone)]
pub struct DoArguments {
    pub(crate) values: Vec<DoArgumentValue>,
}

impl DoArguments {
    pub(crate) fn values(&self) -> &[DoArgumentValue] {
        &self.values
    }
}

impl Arguments for DoArguments {
    type Database = Do;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.reserve(additional);
    }

    fn add<'t, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'t, Do> + Type<Do>,
    {
        add_argument(&mut self.values, value)
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use sqlx_core::arguments::Arguments;

    use super::{DoArgumentValue, DoArguments};

    #[test]
    fn values_line_up_with_placeholders() {
        let mut arguments = DoArguments::default();

        arguments.add(7_i64).unwrap();
        arguments.add(Option::<String>::None).unwrap();
        arguments.add("x").unwrap();
        arguments.add(true).unwrap();
        arguments.add(1.5_f64).unwrap();
        arguments.add(vec![1_u8, 2]).unwrap();

        assert_eq!(
            arguments.values(),
            [
                DoArgumentValue::Integer(7),
                DoArgumentValue::Null,
                DoArgumentValue::Text("x".into()),
                DoArgumentValue::Integer(1),
                DoArgumentValue::Real(1.5),
                DoArgumentValue::Blob(vec![1, 2]),
            ]
        );
    }

    #[test]
    fn an_integer_beyond_2_53_is_refused_and_leaves_no_trace() {
        let mut arguments = DoArguments::default();

        arguments.add(1_i64).unwrap();
        assert!(arguments.add(1_i64 << 53).is_err());
        assert!(arguments.add(i64::MIN).is_err());
        arguments.add(2_i64).unwrap();

        // The failed binds pushed nothing, so `?2` is still the second value.
        assert_eq!(
            arguments.values(),
            [DoArgumentValue::Integer(1), DoArgumentValue::Integer(2)]
        );
    }
}
