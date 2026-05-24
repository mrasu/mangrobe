use crate::domain::model::stream::Stream;
use crate::domain::model::user_table_id::UserTableId;

#[derive(Clone, Debug)]
pub(crate) struct UserTablStream {
    pub user_table_id: UserTableId,
    pub stream: Stream,
}

impl UserTablStream {
    pub fn new(user_table_id: UserTableId, stream: Stream) -> Self {
        Self {
            user_table_id,
            stream,
        }
    }
}
