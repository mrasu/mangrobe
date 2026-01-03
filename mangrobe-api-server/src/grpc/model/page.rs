use crate::grpc::proto::PaginationRequest;
use crate::util::error::ParameterError;

const DEFAULT_PAGE_SIZE_LIMIT: i32 = 10_000;

pub struct Page {
    pub size: i32,
    pub token: Option<String>,
}

pub fn build_page(
    pagination_request: &PaginationRequest,
    default_size: i32,
) -> Result<Page, ParameterError> {
    // Following Google's guidance:
    // > * The page_size field must not be required.
    // > * If the user does not specify page_size (or specifies 0), the API chooses an appropriate default, which the API should document. The API must not return an error.
    // > * If the user specifies page_size greater than the maximum permitted by the API, the API should coerce down to the maximum permitted page size.
    // > * If the user specifies a negative value for page_size, the API must send an INVALID_ARGUMENT error.
    // https://google.aip.dev/158
    let size = match pagination_request.size {
        s if s < 0 => {
            return Err(ParameterError::Invalid(
                "page_size".to_string(),
                "must be positive".to_string(),
            ));
        }
        0 => default_size,
        s => s.min(DEFAULT_PAGE_SIZE_LIMIT),
    };

    let token = if pagination_request.token.is_empty() {
        None
    } else {
        Some(pagination_request.token.clone())
    };

    Ok(Page { size, token })
}
