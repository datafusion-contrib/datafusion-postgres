mod common;

use common::*;
use pgwire::api::query::SimpleQueryHandler;

#[tokio::test]
pub async fn test_dbeaver_startup_sql() {
    let service = setup_handlers();
    let mut client = MockClient::new();

    SimpleQueryHandler::do_query(&service, &mut client, "SELECT 1")
        .await
        .expect("failed to run sql");
}
