//! Parsing and rendering `turso:` connection URLs
//!
//! Accepted forms: `turso:<path>`, `turso://<path>`, `turso::memory:` and `turso:<path>?mode=…`
//! with `mode` one of `ro`, `rw`, `rwc` or `memory`. Only the
//! target and open mode round-trip.

use std::{path::PathBuf, str::FromStr};

use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use sqlx_core::error::Error;
use url::Url;

use super::{TursoConnectOptions, TursoDatabaseTarget};
use crate::TursoAdapterError;

impl FromStr for TursoConnectOptions {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self, Self::Err> {
        let rest = url
            .strip_prefix("turso://")
            .or_else(|| url.strip_prefix("turso:"))
            .ok_or(TursoAdapterError::MissingTursoScheme)?;
        let (database, parameters) = match rest.split_once('?') {
            Some((database, parameters)) => (database, Some(parameters)),
            None => (rest, None),
        };

        let mut options = match database {
            ":memory:" => Self::new().in_memory(),
            path => {
                let decoded = percent_decode_str(path)
                    .decode_utf8()
                    .map_err(|_utf8_error| TursoAdapterError::InvalidUrlPath)?;
                Self::new().filename(PathBuf::from(decoded.as_ref()))
            }
        };

        if let Some(parameters) = parameters {
            options = apply_url_parameters(options, parameters)?;
        }

        Ok(options)
    }
}

fn apply_url_parameters(
    mut options: TursoConnectOptions,
    parameters: &str,
) -> Result<TursoConnectOptions, TursoAdapterError> {
    for (name, value) in url::form_urlencoded::parse(parameters.as_bytes()) {
        options = match (&*name, &*value) {
            ("mode", "rw") => options.create_if_missing(false),
            ("mode", "rwc") => options.create_if_missing(true),
            ("mode", "memory") => options.in_memory(),
            ("mode", "ro") => options.read_only(true),
            ("mode", _) => {
                return Err(TursoAdapterError::InvalidUrlParameterValue {
                    name: "mode",
                    value: value.into_owned(),
                });
            }
            _ => {
                return Err(TursoAdapterError::UnknownUrlParameter {
                    name: name.into_owned(),
                });
            }
        };
    }

    Ok(options)
}

/// Path bytes escaped when rendering: everything `from_str` would otherwise read as URL syntax
/// (`?` starts the parameters, `%` starts an escape, `#` a fragment) plus whitespace, controls
/// and non-ASCII, so the parser's percent-decoding restores the exact path
const PATH_ESCAPES: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

/// Renders the target and open mode as a `turso:` URL that parses back to the same target for
/// any UTF-8 path; a non-UTF-8 path is rendered lossily
pub(super) fn render_connection_url(options: &TursoConnectOptions) -> Url {
    let mut url = empty_turso_url();
    match options.target() {
        TursoDatabaseTarget::File(path) => {
            let path = path.to_string_lossy();
            url.set_path(&utf8_percent_encode(&path, PATH_ESCAPES).to_string());
        }
        TursoDatabaseTarget::Memory => url.set_path(":memory:"),
    }
    url.query_pairs_mut()
        .append_pair("mode", options.open_mode().url_value(options.target()));
    url
}

/// The scheme-only URL `turso:`
///
/// `ConnectOptions::to_url_lossy` must return a `Url` and cannot fail, and every `Url`
/// constructor is fallible. Parsing this constant is the one place that relies on a literal
/// being valid.
#[expect(
    clippy::expect_used,
    reason = "the constant scheme-only URL `turso:` always parses; to_url_lossy cannot fail"
)]
fn empty_turso_url() -> Url {
    Url::parse("turso:").expect("`turso:` is a valid URL")
}

#[cfg(test)]
mod tests {
    use std::{path::Path, str::FromStr};

    use sqlx_core::{connection::ConnectOptions, error::Error};
    use url::Url;

    use crate::{TursoAdapterError, TursoConnectOptions, TursoDatabaseTarget};

    fn adapter_error(error: &Error) -> Option<&TursoAdapterError> {
        match error {
            Error::Configuration(source) => source.downcast_ref::<TursoAdapterError>(),
            _ => None,
        }
    }

    #[test]
    fn parses_memory_urls() -> sqlx_core::Result<()> {
        assert!(TursoConnectOptions::from_str("turso::memory:")?.is_in_memory());
        assert!(TursoConnectOptions::from_str("turso://:memory:")?.is_in_memory());
        assert!(TursoConnectOptions::from_str("turso://?mode=memory")?.is_in_memory());
        Ok(())
    }

    #[test]
    fn parses_file_urls_and_modes() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str("turso:data.db?mode=rwc")?;
        assert_eq!(options.get_filename(), Some(Path::new("data.db")));
        assert!(options.get_create_if_missing());

        let options = TursoConnectOptions::from_str("turso://data.db?mode=rw")?;
        assert!(!options.get_create_if_missing());

        let options = TursoConnectOptions::from_str("turso:///tmp/data%20file.db")?;
        assert_eq!(options.get_filename(), Some(Path::new("/tmp/data file.db")));
        Ok(())
    }

    #[test]
    fn parses_read_only_mode() -> sqlx_core::Result<()> {
        let options = TursoConnectOptions::from_str("turso:data.db?mode=ro")?;
        assert!(options.get_read_only());
        assert!(!options.get_create_if_missing());
        Ok(())
    }

    #[test]
    fn rejects_other_schemes_parameters_and_values() {
        let scheme = TursoConnectOptions::from_str("sqlite://data.db").expect_err("scheme");
        assert_eq!(
            adapter_error(&scheme),
            Some(&TursoAdapterError::MissingTursoScheme)
        );

        let mode = TursoConnectOptions::from_str("turso:data.db?mode=bad").expect_err("mode");
        assert!(matches!(
            adapter_error(&mode),
            Some(TursoAdapterError::InvalidUrlParameterValue { name: "mode", .. })
        ));

        for removed in [
            "cache=shared",
            "sync_remote_url=http%3A%2F%2Fhub",
            "auto_vacuum=FULL",
        ] {
            let url = format!("turso:data.db?{removed}");
            let error = TursoConnectOptions::from_str(&url).expect_err("removed parameter");
            assert!(matches!(
                adapter_error(&error),
                Some(TursoAdapterError::UnknownUrlParameter { .. })
            ));
        }
    }

    #[test]
    fn url_round_trips_target_and_mode() -> sqlx_core::Result<()> {
        for options in [
            TursoConnectOptions::new(),
            TursoConnectOptions::new().filename("/tmp/store dir/project.db"),
            TursoConnectOptions::new().filename("/tmp/a%20b.db"),
            TursoConnectOptions::new().filename("/tmp/what?.db"),
            TursoConnectOptions::new().filename("/tmp/hash#tag/100%.db"),
            TursoConnectOptions::new().filename("/tmp/caf\u{e9}/\u{4e2d}.db"),
            TursoConnectOptions::new()
                .filename("retained.db")
                .read_only(true),
            TursoConnectOptions::new()
                .filename("relative.db")
                .create_if_missing(true),
        ] {
            // Act
            let url = options.to_url_lossy();
            let parsed = <TursoConnectOptions as ConnectOptions>::from_url(&url)?;

            // Assert
            assert!(!url.as_str().contains(' '), "{url}");
            assert_eq!(parsed.target(), options.target(), "{url}");
            assert_eq!(
                parsed.get_create_if_missing(),
                options.get_create_if_missing(),
                "{url}"
            );
            assert_eq!(parsed.get_read_only(), options.get_read_only(), "{url}");
        }
        Ok(())
    }

    #[test]
    fn from_url_accepts_a_parsed_url() -> sqlx_core::Result<()> {
        // Arrange
        let url = Url::parse("turso:/var/stores/project.db?mode=rwc").map_err(Error::config)?;

        // Act
        let options = <TursoConnectOptions as ConnectOptions>::from_url(&url)?;

        // Assert
        assert_eq!(
            options.target(),
            &TursoDatabaseTarget::File("/var/stores/project.db".into())
        );
        assert!(options.get_create_if_missing());
        Ok(())
    }
}
