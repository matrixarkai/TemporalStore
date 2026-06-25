use crate::types::{CommandResponse, ExecuteResponse, Status};

pub(super) fn execute_error(
    code: impl Into<String>,
    message: impl Into<String>,
) -> ExecuteResponse {
    ExecuteResponse {
        status: Status::error(code, message),
        response: CommandResponse::Empty,
    }
}
