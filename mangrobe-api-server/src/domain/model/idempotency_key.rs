#[derive(Clone)]
pub struct IdempotencyKey {
    val: Vec<u8>,
}

impl TryFrom<Vec<u8>> for IdempotencyKey {
    type Error = String;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > 16 {
            return Err("length must be 1 to 16".into());
        }

        Ok(Self { val: value })
    }
}

impl IdempotencyKey {
    pub fn vec(&self) -> Vec<u8> {
        self.val.clone()
    }
}
