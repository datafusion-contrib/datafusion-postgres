use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::auth::{AuthManager, AuthConfig, User};
use datafusion_postgres::{serve_with_auth, ServerOptions};

/// Example server that demonstrates basic authentication without password requirements
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Create a basic auth configuration (no password requirements)
    let basic_config = AuthConfig {
        require_passwords: false,  // Disable password requirements for testing
        allow_empty_passwords: true,
    };

    // Create auth manager with basic configuration
    let auth_manager = AuthManager::new_with_config(basic_config);

    // Add custom users without passwords
    let admin_user = User {
        username: "admin".to_string(),
        password_hash: "".to_string(), // Empty password for basic test
        roles: vec!["dbadmin".to_string()],
        is_superuser: true,
        can_login: true,
        connection_limit: None,
    };

    let auth_manager = Arc::new(auth_manager);

    // Add users to auth manager
    auth_manager.add_user(admin_user).await?;

    println!("🔓 Basic authentication server configured");
    println!("📋 Available users (no password required):");
    for user in auth_manager.list_users().await {
        println!("  - {}", user);
    }
    println!("");
    println!("🔒 Authentication Config:");
    println!("  - require_passwords: {}", auth_manager.get_config().require_passwords);
    println!("  - allow_empty_passwords: {}", auth_manager.get_config().allow_empty_passwords);

    // Create session context
    let session_context = Arc::new(SessionContext::new());

    // Create server options
    let server_options = ServerOptions::new()
        .with_host("127.0.0.1".to_string())
        .with_port(5441); // Different port to avoid conflicts

    println!("");
    println!("🚀 Starting basic server on port 5441...");
    println!("🔓 No password required for connections:");
    println!("  - postgres (no password)");
    println!("  - admin (no password)");
    println!("");
    println!("💡 Test connections:");
    println!("  psql -h 127.0.0.1 -p 5441 -U postgres");
    println!("  psql -h 127.0.0.1 -p 5441 -U admin");
    
    // Start server with basic auth manager
    serve_with_auth(session_context, Some(auth_manager), &server_options).await?;

    Ok(())
}