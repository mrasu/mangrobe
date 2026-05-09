use crate::api::core::util::error::ParameterError;
use crate::domain::model::idempotency_key::IdempotencyKey;

pub fn to_idempotency_key(
    param: Option<crate::api::grpc::proto::IdempotencyKey>,
) -> Result<IdempotencyKey, ParameterError> {
    let Some(param) = param else {
        return Err(ParameterError::Required("idempotency_key".to_string()));
    };

    IdempotencyKey::try_from(param.key)
        .map_err(|msg| ParameterError::Invalid("idempotency_key".to_string(), msg))
}
