
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::auth::{AuthManager, User};
use datafusion_postgres::{serve_with_auth, ServerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Create a custom auth manager
    let auth_manager = Arc::new(AuthManager::new());

    // Add custom users
    let admin_user = User {
        username: "admin".to_string(),
        password_hash: "admin_password".to_string(),
        roles: vec!["dbadmin".to_string()],
        is_superuser: true,
        can_login: true,
        connection_limit: None,
    };

    let readonly_user = User {
        username: "reader".to_string(),
        password_hash: "reader_password".to_string(),
        roles: vec!["readonly".to_string()],
        is_superuser: false,
        can_login: true,
        connection_limit: Some(5),
    };

    // Add users to auth manager
    auth_manager.add_user(admin_user).await?;
    auth_manager.add_user(readonly_user).await?;

    // Wait a bit for async user addition
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("Custom auth manager configured with users:");
    for user in auth_manager.list_users().await {
        println!("  - {}", user);
    }

    // Create session context
    let session_context = Arc::new(SessionContext::new());

    // Create server options
    let server_options = ServerOptions::new()
        .with_host("127.0.0.1".to_string())
        .with_port(5437);

    println!("Starting server with custom authentication on port 5437...");
    
    // Start server with custom auth manager
    serve_with_auth(
        session_context,
        Some(auth_manager),
        &server_options,
    )
    .await?;

    Ok(())
}
