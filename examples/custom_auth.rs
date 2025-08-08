use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_postgres::auth::{AuthManager, User, RoleConfig};
use datafusion_postgres::{serve_with_auth, ServerOptions};

/// Example demonstrating how to use a custom auth manager with datafusion-postgres
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

    // Create a custom role
    auth_manager
        .create_role(RoleConfig {
            name: "custom_role".to_string(),
            is_superuser: false,
            can_login: false,
            can_create_db: false,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
        })
        .await?;

    println!("Custom auth manager configured with users:");
    for user in auth_manager.list_users().await {
        println!("  - {}", user);
    }

    // Create session context
    let session_context = Arc::new(SessionContext::new());

    // Create server options
    let server_options = ServerOptions::new()
        .with_host("127.0.0.1".to_string())
        .with_port(5433); // Use different port to avoid conflicts

    // Start server with custom auth manager
    println!("Starting server with custom authentication on port 5433...");
    println!("You can connect as:");
    println!("  - admin (superuser)");
    println!("  - reader (readonly access)");
    
    serve_with_auth(
        session_context,
        Some(auth_manager),
        &server_options,
    )
    .await?;

    Ok(())
}