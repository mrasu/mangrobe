use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
// String = [A-Za-z_][A-Za-z0-9_]*
pub(crate) struct DbObjectIdentifier(String);

impl TryFrom<String> for DbObjectIdentifier {
    type Error = DbObjectIdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut chars = value.chars();
        let Some(first) = chars.next() else {
            return Err(DbObjectIdentifierError::Empty);
        };

        if !is_identifier_start(first) {
            return Err(DbObjectIdentifierError::ContainsInvalidChar {
                value,
                invalid_char: first,
            });
        }

        for c in chars {
            if !is_identifier_part(c) {
                return Err(DbObjectIdentifierError::ContainsInvalidChar {
                    value,
                    invalid_char: c,
                });
            }
        }

        Ok(Self(value))
    }
}

impl DbObjectIdentifier {
    pub fn val(&self) -> String {
        self.0.clone()
    }
}

#[derive(Error, Debug)]
pub(crate) enum DbObjectIdentifierError {
    #[error("identifier must not be empty")]
    Empty,

    #[error(
        "identifier '{value}' character must match [A-Za-z_][A-Za-z0-9_]*, but got '{invalid_char}'"
    )]
    ContainsInvalidChar { value: String, invalid_char: char },
}

fn is_identifier_start(c: char) -> bool {
    c == '_' || c.is_ascii_alphabetic()
}

fn is_identifier_part(c: char) -> bool {
    c == '_' || c.is_ascii_alphanumeric()
}
