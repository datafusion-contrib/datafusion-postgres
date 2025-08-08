use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::auth::{AuthManager, AuthConfig, User};
use datafusion_postgres::{serve_with_auth, ServerOptions};

/// Example server that demonstrates secure authentication with password requirements
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Create a secure auth configuration requiring passwords
    let secure_config = AuthConfig {
        require_passwords: true,
        allow_empty_passwords: false,
    };

    // Create auth manager with secure configuration
    let auth_manager = AuthManager::new_with_config(secure_config);

    // Set a password for the default postgres user
    auth_manager.set_postgres_password("secure_postgres_password").await?;

    // Add custom users with required passwords
    let admin_user = User {
        username: "admin".to_string(),
        password_hash: "admin_secure_pass".to_string(),
        roles: vec!["dbadmin".to_string()],
        is_superuser: true,
        can_login: true,
        connection_limit: None,
    };

    let readonly_user = User {
        username: "reader".to_string(),
        password_hash: "reader_secure_pass".to_string(),
        roles: vec!["readonly".to_string()],
        is_superuser: false,
        can_login: true,
        connection_limit: Some(10),
    };

    let auth_manager = Arc::new(auth_manager);

    // Add users to auth manager
    auth_manager.add_user(admin_user).await?;
    auth_manager.add_user(readonly_user).await?;

    println!("🔐 Secure authentication server configured with password requirements");
    println!("📋 Available users (all require passwords):");
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
        .with_port(5440); // Different port to avoid conflicts

    println!("");
    println!("🚀 Starting secure server on port 5440...");
    println!("🔐 Password-protected connections required:");
    println!("  - postgres (password: secure_postgres_password)");
    println!("  - admin (password: admin_secure_pass)");
    println!("  - reader (password: reader_secure_pass)");
    println!("");
    println!("⚠️  This example demonstrates password enforcement configuration!");
    println!("");
    println!("📚 NOTE: Full password enforcement requires proper pgwire authentication handlers.");
    println!("         This example shows the AuthConfig API and password requirement setup.");
    println!("         For production, integrate with pgwire CleartextStartupHandler or MD5StartupHandler.");
    println!("");
    println!("💡 Test connections (will show password configuration):");
    println!("  psql -h 127.0.0.1 -p 5440 -U postgres      # Shows secure auth manager setup");
    println!("  psql -h 127.0.0.1 -p 5440 -U admin         # Shows custom user with password");
    println!("  psql -h 127.0.0.1 -p 5440 -U reader        # Shows readonly user with password");
    
    // Start server with secure auth manager
    serve_with_auth(session_context, Some(auth_manager), &server_options).await?;

    Ok(())
}