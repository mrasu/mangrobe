use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::partition::to_unvalidated_partition;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::partition_predicate::Predicate;
use crate::api::grpc::proto::{
    BoundInclusivity as BoundInclusivityParam, GetCurrentStateRequest,
    PartitionBound as PartitionBoundParam, PartitionFilter as PartitionFilterParam,
    PartitionPredicate as PartitionPredicateParam, PartitionRange as PartitionRangeParam,
};
use crate::application::data_manipulation::get_current_state_param::GetCurrentStateParam;
use crate::domain::model::partition_filter::{
    BoundInclusivity, UnvalidatedPartitionBound, UnvalidatedPartitionFilter,
    UnvalidatedPartitionIn, UnvalidatedPartitionPredicate, UnvalidatedPartitionRange,
};

pub(crate) fn build_get_current_state_param(
    req: &GetCurrentStateRequest,
) -> Result<GetCurrentStateParam, ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

    let param = GetCurrentStateParam {
        table_identifier,
        stream: req.stream.into(),
        unvalidated_partition_filter: to_unvalidated_partition_filter(
            req.partition_filter.as_ref(),
        )?,
    };
    Ok(param)
}

fn to_unvalidated_partition_filter(
    param: Option<&PartitionFilterParam>,
) -> Result<UnvalidatedPartitionFilter, ParameterError> {
    let Some(param) = param else {
        return Ok(UnvalidatedPartitionFilter { predicates: vec![] });
    };

    let predicates = param
        .predicates
        .iter()
        .map(to_unvalidated_partition_predicate)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(UnvalidatedPartitionFilter::new(predicates))
}

fn to_unvalidated_partition_predicate(
    param: &PartitionPredicateParam,
) -> Result<UnvalidatedPartitionPredicate, ParameterError> {
    let predicate = param.predicate.as_ref().ok_or(ParameterError::Required(
        "partition_filter.predicates.predicate".to_string(),
    ))?;

    match predicate {
        Predicate::In(param) => {
            let partitions = param
                .partitions
                .iter()
                .map(|partition| to_unvalidated_partition(Some(partition)))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(UnvalidatedPartitionPredicate::In(UnvalidatedPartitionIn {
                partitions,
            }))
        }
        Predicate::Range(param) => Ok(UnvalidatedPartitionPredicate::Range(
            to_unvalidated_partition_range(param)?,
        )),
    }
}

fn to_unvalidated_partition_range(
    param: &PartitionRangeParam,
) -> Result<UnvalidatedPartitionRange, ParameterError> {
    Ok(UnvalidatedPartitionRange {
        lower: param
            .lower
            .as_ref()
            .map(to_unvalidated_partition_bound)
            .transpose()?,
        upper: param
            .upper
            .as_ref()
            .map(to_unvalidated_partition_bound)
            .transpose()?,
    })
}

fn to_unvalidated_partition_bound(
    param: &PartitionBoundParam,
) -> Result<UnvalidatedPartitionBound, ParameterError> {
    let inclusivity = match BoundInclusivityParam::try_from(param.inclusivity) {
        Ok(BoundInclusivityParam::Inclusive) => BoundInclusivity::Inclusive,
        Ok(BoundInclusivityParam::Exclusive) => BoundInclusivity::Exclusive,
        Ok(BoundInclusivityParam::Unspecified) | Err(_) => {
            return Err(ParameterError::Invalid(
                "partition_filter.predicates.range.bound.inclusivity".to_string(),
                "invalid".to_string(),
            ));
        }
    };

    Ok(UnvalidatedPartitionBound {
        partition: to_unvalidated_partition(param.partition.as_ref())?,
        inclusivity,
    })
}
