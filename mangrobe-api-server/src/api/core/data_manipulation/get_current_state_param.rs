use crate::api::core::util::error::ParameterError;
use crate::api::core::util::param::table_identifier::to_table_identifier;
use crate::api::core::util::param_util::required;
use crate::api::grpc::proto::partition_time_predicate::Predicate;
use crate::api::grpc::proto::{
    BoundInclusivity as BoundInclusivityParam, GetCurrentStateRequest,
    PartitionTimeBound as PartitionTimeBoundParam, PartitionTimeFilter as PartitionTimeFilterParam,
    PartitionTimePredicate as PartitionTimePredicateParam,
    PartitionTimeRange as PartitionTimeRangeParam,
};
use crate::application::data_manipulation::get_current_state_param::GetCurrentStateParam;
use crate::domain::model::partition_time_filter::{
    BoundInclusivity, PartitionTimeBound, PartitionTimeFilter, PartitionTimeIn,
    PartitionTimePredicate, PartitionTimeRange,
};
use chrono::{DateTime, Utc};
use prost_types::Timestamp;

pub(crate) fn build_get_current_state_param(
    req: &GetCurrentStateRequest,
) -> Result<GetCurrentStateParam, ParameterError> {
    let table_identifier =
        to_table_identifier(required("table_identifier", req.table_identifier.as_ref())?)?;

    let param = GetCurrentStateParam {
        table_identifier,
        stream_id: req.stream_id.into(),
        partition_time_filter: to_partition_time_filter(req.partition_time_filter.as_ref())?,
    };
    Ok(param)
}

fn to_partition_time_filter(
    param: Option<&PartitionTimeFilterParam>,
) -> Result<PartitionTimeFilter, ParameterError> {
    let Some(param) = param else {
        return Ok(PartitionTimeFilter { predicates: vec![] });
    };

    let predicates = param
        .predicates
        .iter()
        .map(to_partition_time_predicate)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PartitionTimeFilter::new(predicates))
}

fn to_partition_time_predicate(
    param: &PartitionTimePredicateParam,
) -> Result<PartitionTimePredicate, ParameterError> {
    let predicate = param.predicate.as_ref().ok_or(ParameterError::Required(
        "partition_time_filter.predicates.predicate".to_string(),
    ))?;

    match predicate {
        Predicate::In(param) => {
            let times = param
                .times
                .iter()
                .map(|time| to_partition_time_ref(Some(time)))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(PartitionTimePredicate::In(PartitionTimeIn { times }))
        }
        Predicate::Range(param) => Ok(PartitionTimePredicate::Range(to_partition_time_range(
            param,
        )?)),
    }
}

fn to_partition_time_range(
    param: &PartitionTimeRangeParam,
) -> Result<PartitionTimeRange, ParameterError> {
    Ok(PartitionTimeRange {
        lower: param
            .lower
            .as_ref()
            .map(to_partition_time_bound)
            .transpose()?,
        upper: param
            .upper
            .as_ref()
            .map(to_partition_time_bound)
            .transpose()?,
    })
}

fn to_partition_time_bound(
    param: &PartitionTimeBoundParam,
) -> Result<PartitionTimeBound, ParameterError> {
    let inclusivity = match BoundInclusivityParam::try_from(param.inclusivity) {
        Ok(BoundInclusivityParam::Inclusive) => BoundInclusivity::Inclusive,
        Ok(BoundInclusivityParam::Exclusive) => BoundInclusivity::Exclusive,
        Ok(BoundInclusivityParam::Unspecified) | Err(_) => {
            return Err(ParameterError::Invalid(
                "partition_time_filter.predicates.range.bound.inclusivity".to_string(),
                "invalid".to_string(),
            ));
        }
    };

    Ok(PartitionTimeBound {
        time: to_partition_time_ref(param.time.as_ref())?,
        inclusivity,
    })
}

fn to_partition_time_ref(param: Option<&Timestamp>) -> Result<DateTime<Utc>, ParameterError> {
    let req_partition_time = param.ok_or(ParameterError::Required("partition_time".into()))?;

    DateTime::from_timestamp(req_partition_time.seconds, req_partition_time.nanos as u32).ok_or(
        ParameterError::Invalid(
            "partition_time".to_string(),
            "out-of-range number of seconds or nanos".to_string(),
        ),
    )
}
