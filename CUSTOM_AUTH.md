# Custom Authentication Manager

This document describes how to use the custom authentication manager feature in datafusion-postgres.

## Overview

The datafusion-postgres library now supports custom authentication managers, allowing you to provide your own user authentication and authorization logic instead of relying on the default empty authentication manager.

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
use datafusion_postgres::auth::{AuthManager, User, RoleConfig};
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

## See Also

- [Integration Tests](tests-integration/README.md) - Comprehensive test documentation
- [Examples](examples/custom_auth.rs) - Example implementation
- [API Documentation](datafusion-postgres/src/auth.rs) - Full API reference