use crate::DigestError;
use turso_parser::{
    ast::{
        Name,
        fmt::{BlankContext, ToTokens, TokenStream, WriteTokenStream},
    },
    parser::Parser,
    token::TokenType,
};
pub(crate) fn canonical_schema_sql(sql: &str) -> Result<String, DigestError> {
    let mut parser = Parser::new(sql.as_bytes());
    let command = parser
        .next_cmd()
        .map_err(|e| DigestError::Encoding(e.to_string()))?
        .ok_or_else(|| DigestError::Encoding("empty schema SQL".into()))?;
    if parser
        .next_cmd()
        .map_err(|e| DigestError::Encoding(e.to_string()))?
        .is_some()
    {
        return Err(DigestError::Encoding("multiple schema commands".into()));
    }
    let mut rendered = String::new();
    let mut stream = CanonicalIdentifiers {
        inner: WriteTokenStream::new(&mut rendered),
    };
    command
        .to_tokens(&mut stream, &BlankContext)
        .map_err(|e| DigestError::Encoding(e.to_string()))?;
    Ok(rendered)
}
struct CanonicalIdentifiers<'a> {
    inner: WriteTokenStream<'a, String>,
}
impl TokenStream for CanonicalIdentifiers<'_> {
    type Error = std::fmt::Error;
    fn append(&mut self, kind: TokenType, value: Option<&str>) -> Result<(), Self::Error> {
        if kind == TokenType::TK_ID
            && let Some(value) = value
        {
            let identifier = Name::exact(Name::from_string(value).as_str().to_owned()).as_ident();
            return self.inner.append(kind, Some(&identifier));
        }
        self.inner.append(kind, value)
    }
}
