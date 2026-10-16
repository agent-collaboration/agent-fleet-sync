//! Lexical inspection of SQL text before it reaches the engine
//!
//! Finds whether the text may hold more than one statement (it then runs as a batch, which
//! takes no arguments) and whether it uses a named placeholder (`:name`, `@name`, `$name`),
//! which the argument buffer cannot bind. Quoted strings, quoted identifiers and comments are
//! skipped, so `';'` inside a literal is not a statement separator.

use std::iter::Peekable;

#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct SqlInspection {
    pub(super) may_contain_multiple_statements: bool,
    pub(super) unsupported_named_placeholder: Option<String>,
}

pub(super) fn inspect_sql(sql: &str) -> SqlInspection {
    let mut inspection = SqlInspection::default();
    let mut saw_statement = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                saw_statement = true;
                skip_single_quoted(&mut chars);
            }
            '"' | '`' => {
                saw_statement = true;
                skip_until(&mut chars, ch);
            }
            '[' => {
                saw_statement = true;
                skip_until(&mut chars, ']');
            }
            '-' if chars.next_if_eq(&'-').is_some() => skip_line_comment(&mut chars),
            '/' if chars.next_if_eq(&'*').is_some() => skip_block_comment(&mut chars),
            ';' if saw_statement && has_remaining_statement_text(chars.clone()) => {
                inspection.may_contain_multiple_statements = true;
                return inspection;
            }
            '$' => {
                saw_statement = true;
                let name = read_placeholder_name(&mut chars);
                if name.parse::<usize>().is_err() {
                    inspection.unsupported_named_placeholder = Some(format!("${name}"));
                }
            }
            ':' | '@' => {
                saw_statement = true;
                let name = read_placeholder_name(&mut chars);
                if !name.is_empty() {
                    inspection.unsupported_named_placeholder = Some(format!("{ch}{name}"));
                }
            }
            ch if !ch.is_whitespace() => saw_statement = true,
            _ => {}
        }
    }

    inspection
}

fn read_placeholder_name<I>(chars: &mut Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut name = String::new();
    while let Some(ch) = chars.next_if(|ch| ch.is_ascii_alphanumeric() || *ch == '_') {
        name.push(ch);
    }
    name
}

/// Skips a single-quoted literal, where `''` is an escaped quote
fn skip_single_quoted<I>(chars: &mut Peekable<I>)
where
    I: Iterator<Item = char>,
{
    while let Some(quoted) = chars.next() {
        if quoted == '\'' && chars.next_if_eq(&'\'').is_none() {
            break;
        }
    }
}

fn skip_until<I>(chars: &mut Peekable<I>, terminator: char)
where
    I: Iterator<Item = char>,
{
    for quoted in chars.by_ref() {
        if quoted == terminator {
            break;
        }
    }
}

fn skip_line_comment<I>(chars: &mut Peekable<I>)
where
    I: Iterator<Item = char>,
{
    skip_until(chars, '\n');
}

fn skip_block_comment<I>(chars: &mut Peekable<I>)
where
    I: Iterator<Item = char>,
{
    let mut previous = '\0';
    for comment in chars.by_ref() {
        if previous == '*' && comment == '/' {
            break;
        }
        previous = comment;
    }
}

fn has_remaining_statement_text<I>(chars: I) -> bool
where
    I: Iterator<Item = char>,
{
    let mut chars = chars.peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '-' if chars.next_if_eq(&'-').is_some() => skip_line_comment(&mut chars),
            '/' if chars.next_if_eq(&'*').is_some() => skip_block_comment(&mut chars),
            ch if !ch.is_whitespace() && ch != ';' => return true,
            _ => {}
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::{SqlInspection, inspect_sql};

    fn single_statement() -> SqlInspection {
        SqlInspection::default()
    }

    #[test]
    fn a_trailing_semicolon_or_comment_is_still_one_statement() {
        assert_eq!(inspect_sql("SELECT 1;"), single_statement());
        assert_eq!(inspect_sql("SELECT 1; -- done"), single_statement());
        assert_eq!(inspect_sql("SELECT 1; /* done */ ;"), single_statement());
    }

    #[test]
    fn semicolons_inside_literals_identifiers_and_comments_do_not_split() {
        assert_eq!(inspect_sql("SELECT ';' AS separator"), single_statement());
        assert_eq!(inspect_sql("SELECT 'it''s; fine'"), single_statement());
        assert_eq!(inspect_sql("SELECT \"a;b\" FROM [c;d]"), single_statement());
        assert_eq!(inspect_sql("SELECT 1 -- a; b\n"), single_statement());
    }

    #[test]
    fn two_statements_are_detected() {
        assert!(
            inspect_sql("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1)")
                .may_contain_multiple_statements
        );
    }

    #[test]
    fn numbered_placeholders_are_supported_and_named_ones_are_reported() {
        assert_eq!(inspect_sql("SELECT ?, ?2, $3"), single_statement());
        assert_eq!(
            inspect_sql("SELECT :name")
                .unsupported_named_placeholder
                .as_deref(),
            Some(":name")
        );
        assert_eq!(
            inspect_sql("SELECT @name")
                .unsupported_named_placeholder
                .as_deref(),
            Some("@name")
        );
        assert_eq!(
            inspect_sql("SELECT $name")
                .unsupported_named_placeholder
                .as_deref(),
            Some("$name")
        );
    }

    #[test]
    fn placeholders_inside_literals_are_ignored() {
        assert_eq!(inspect_sql("SELECT ':name', '@x'"), single_statement());
    }
}
