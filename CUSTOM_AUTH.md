# Custom Authentication Manager

This document describes how to use the custom authentication manager feature in datafusion-postgres.

## Overview

The datafusion-postgres library now supports custom authentication managers with configurable password requirements, allowing you to:

- Provide your own user authentication and authorization logic
- **Require passwords for all users** (including the default postgres user)
- Configure whether empty passwords are allowed
- Enforce secure authentication policies

## API Changes

### New Function: `serve_with_auth`

```rust
pub async fn serve_with_auth(
    session_context: Arc<SessionContext>,
    auth_manager: Option<Arc<AuthManager>>,
    opts: &ServerOptions,
) -> Result<(), std::io::Error>
```

### Backward Compatibility

The original `serve` function remains unchanged and continues to work:

```rust
pub async fn serve(
    session_context: Arc<SessionContext>,
    opts: &ServerOptions,
) -> Result<(), std::io::Error>
```

This function now internally calls `serve_with_auth(session_context, None, opts)`.

## Usage Examples

### Basic Usage with Default Auth Manager

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::{serve, ServerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session_context = Arc::new(SessionContext::new());
    let server_options = ServerOptions::new();
    
    // Uses default auth manager (empty)
    serve(session_context, &server_options).await?;
    Ok(())
}
```

### Custom Auth Manager Usage

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::auth::{AuthManager, AuthConfig, User, RoleConfig};
use datafusion_postgres::{serve_with_auth, ServerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create custom auth manager
    let auth_manager = Arc::new(AuthManager::new());

    // Add custom users
    let admin_user = User {
        username: "admin".to_string(),
        password_hash: "secure_password".to_string(),
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
        connection_limit: Some(10),
    };

    // Add users to auth manager
    auth_manager.add_user(admin_user).await?;
    auth_manager.add_user(readonly_user).await?;

    // Create custom roles
    auth_manager.create_role(RoleConfig {
        name: "custom_role".to_string(),
        is_superuser: false,
        can_login: false,
        can_create_db: true,
        can_create_role: false,
        can_create_user: false,
        can_replication: false,
    }).await?;

    let session_context = Arc::new(SessionContext::new());
    let server_options = ServerOptions::new();
    
    // Use custom auth manager
    serve_with_auth(session_context, Some(auth_manager), &server_options).await?;
    Ok(())
}
```

### Secure Authentication with Password Requirements

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::auth::{AuthManager, AuthConfig, User};
use datafusion_postgres::{serve_with_auth, ServerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create secure auth configuration requiring passwords
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

    auth_manager.add_user(admin_user).await?;

    let session_context = Arc::new(SessionContext::new());
    let server_options = ServerOptions::new();
    
    // Use secure auth manager - passwords now required for ALL users
    serve_with_auth(session_context, Some(Arc::new(auth_manager)), &server_options).await?;
    Ok(())
}
```

## Authentication Manager Features

The `AuthManager` provides comprehensive user and role management:

### User Management
- Add, remove, and modify users
- Password-based authentication
- User roles and permissions
- Connection limits per user
- Superuser privileges

### Role Management
- Create custom roles
- Role inheritance
- Granular permissions (SELECT, INSERT, UPDATE, DELETE, etc.)
- Resource-level access control (tables, schemas, databases)

### Permission System
- Query-level permission checking
- Role-based access control (RBAC)
- Grant and revoke permissions
- WITH GRANT OPTION support

### Password Enforcement
- **Configurable password requirements** - require passwords for all users
- **Secure postgres user** - can require password for default postgres user
- **Empty password control** - allow or disallow empty passwords
- **Authentication policies** - flexible security configuration

## Security Features

### Authentication
- User/password verification
- Role-based authorization
- Superuser privilege checking
- Connection limit enforcement

### Access Control
- Per-query permission validation
- Resource-level access control
- Role inheritance
- Grant/revoke permission management

## Integration Testing

The feature includes comprehensive integration tests:

```bash
# Run all integration tests (including custom auth)
./tests-integration/test.sh

# Run only custom auth tests
./tests-integration/run_custom_auth_test.sh

# Run custom auth test directly
python3 tests-integration/test_custom_auth.py
```

## Migration Guide

### From Default Auth to Custom Auth

1. **No code changes required** for existing applications using the `serve` function
2. **Optional migration** to `serve_with_auth` for custom authentication
3. **Backward compatible** - existing code continues to work unchanged

### Example Migration

Before:
```rust
serve(session_context, &server_options).await?;
```

After (optional):
```rust
let custom_auth = Arc::new(AuthManager::new());
// Configure custom_auth...
serve_with_auth(session_context, Some(custom_auth), &server_options).await?;
```

## Performance Considerations

- The custom auth manager uses `Arc<RwLock<>>` for thread-safe access
- User and role lookups are in-memory hash maps
- Permission checking is performed per query
- Minimal performance impact for simple authentication schemes

## Future Extensions

The authentication system can be extended to support:
- External authentication providers (LDAP, OAuth, etc.)
- Database-backed user storage
- Advanced password policies
- Audit logging
- Session management

## Examples and Testing

### **Basic Custom Auth Demo**
```bash
# Build and run custom auth server (allows passwordless postgres)
cargo run --example custom_auth_server --manifest-path datafusion-postgres/Cargo.toml

# Connect with different users
psql -h 127.0.0.1 -p 5439 -U postgres   # Default superuser (no password)
psql -h 127.0.0.1 -p 5439 -U admin      # Custom admin (password: admin_password)
psql -h 127.0.0.1 -p 5439 -U reader     # Custom reader (password: reader_password)
```

### **Secure Auth Demo (Passwords Required)**
```bash
# Build and run secure auth server (requires passwords for ALL users)
cargo run --example secure_auth_server --manifest-path datafusion-postgres/Cargo.toml

# Connect with required passwords
psql -h 127.0.0.1 -p 5440 -U postgres   # Requires password: secure_postgres_password
psql -h 127.0.0.1 -p 5440 -U admin      # Requires password: admin_secure_pass
psql -h 127.0.0.1 -p 5440 -U reader     # Requires password: reader_secure_pass

# Test password enforcement
python3 tests-integration/test_password_enforcement.py
```

### **Quick Testing Scripts**
```bash
# Test basic custom auth functionality
./test_custom_auth_example.sh

# Test secure auth with password requirements  
./test_secure_auth_example.sh

# Run integration tests
./tests-integration/test.sh
```

## See Also

- [Integration Tests](tests-integration/README.md) - Comprehensive test documentation
- [Examples](examples/) - Custom and secure auth server examples
- [API Documentation](datafusion-postgres/src/auth.rs) - Full API reference