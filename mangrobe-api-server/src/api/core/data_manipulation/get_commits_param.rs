use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::GetCommitsRequest;
use crate::application::data_manipulation::get_commits_param::GetCommitsParam;

pub(crate) fn build_get_commits_param(
    req: &GetCommitsRequest,
) -> Result<GetCommitsParam, ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

    let commit_id_after = if let Some(commit_id_after) = &req.commit_id_after {
        let commit_id_after = commit_id_after.parse::<i64>().map_err(|_| {
            ParameterError::Invalid("commit_id_after".to_string(), "invalid number".to_string())
        })?;
        if commit_id_after < 0 {
            return Err(ParameterError::Invalid(
                "commit_id_after".to_string(),
                "must be non-negative".to_string(),
            ));
        }
        commit_id_after
    } else {
        0
    };

    Ok(GetCommitsParam {
        table_identifier,
        stream: req.stream.into(),
        commit_id_after: commit_id_after.into(),
    })
}
