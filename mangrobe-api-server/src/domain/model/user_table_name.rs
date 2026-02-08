use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
#[serde(transparent)]
pub struct UserTableName(String);

impl TryFrom<String> for UserTableName {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(format!("Empty user table name: {}", value));
        }

        Ok(UserTableName(value))
    }
}

impl UserTableName {
    pub fn val(&self) -> String {
        self.0.clone()
    }
}
