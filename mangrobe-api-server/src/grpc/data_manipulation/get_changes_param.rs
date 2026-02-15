use crate::application::data_manipulation::get_changes_param::GetChangesParam;
use crate::grpc::proto::GetCommitsRequest;
use crate::grpc::util::param_util::to_table_name;
use crate::util::error::ParameterError;
use tonic::Request;

pub(super) fn build_get_commits_param(
    request: Request<GetCommitsRequest>,
) -> Result<GetChangesParam, ParameterError> {
    let req = request.get_ref();
    let table_name = to_table_name(req.table_name.clone())?;

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

    Ok(GetChangesParam {
        table_name,
        stream_id: req.stream_id.into(),
        commit_id_after: commit_id_after.into(),
    })
}
