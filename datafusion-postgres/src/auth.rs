use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::warn;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::{PgWireError, PgWireResult};
use tokio::sync::RwLock;

/// User information stored in the authentication system
#[derive(Debug, Clone)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub is_superuser: bool,
    pub can_login: bool,
    pub connection_limit: Option<i32>,
}

/// Permission types for granular access control
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Index,
    References,
    Trigger,
    Execute,
    Usage,
    Connect,
    Temporary,
    All,
}

impl Permission {
    pub fn from_string(s: &str) -> Option<Permission> {
        match s.to_uppercase().as_str() {
            "SELECT" => Some(Permission::Select),
            "INSERT" => Some(Permission::Insert),
            "UPDATE" => Some(Permission::Update),
            "DELETE" => Some(Permission::Delete),
            "CREATE" => Some(Permission::Create),
            "DROP" => Some(Permission::Drop),
            "ALTER" => Some(Permission::Alter),
            "INDEX" => Some(Permission::Index),
            "REFERENCES" => Some(Permission::References),
            "TRIGGER" => Some(Permission::Trigger),
            "EXECUTE" => Some(Permission::Execute),
            "USAGE" => Some(Permission::Usage),
            "CONNECT" => Some(Permission::Connect),
            "TEMPORARY" => Some(Permission::Temporary),
            "ALL" => Some(Permission::All),
            _ => None,
        }
    }
}

/// Resource types for access control
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Table(String),
    Schema(String),
    Database(String),
    Function(String),
    Sequence(String),
    All,
}

/// Grant entry for specific permissions on resources
#[derive(Debug, Clone)]
pub struct Grant {
    pub permission: Permission,
    pub resource: ResourceType,
    pub granted_by: String,
    pub with_grant_option: bool,
}

/// Role information for access control
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub is_superuser: bool,
    pub can_login: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub can_create_user: bool,
    pub can_replication: bool,
    pub grants: Vec<Grant>,
    pub inherited_roles: Vec<String>,
}

/// Role configuration for creation
#[derive(Debug, Clone)]
pub struct RoleConfig {
    pub name: String,
    pub is_superuser: bool,
    pub can_login: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub can_create_user: bool,
    pub can_replication: bool,
}

/// Authentication configuration options
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether to require passwords for all users (including postgres)
    pub require_passwords: bool,
    /// Whether to allow empty passwords (when require_passwords is false)
    pub allow_empty_passwords: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            require_passwords: false,
            allow_empty_passwords: true,
        }
    }
}

/// Authentication manager that handles users and roles
#[derive(Debug)]
pub struct AuthManager {
    users: Arc<RwLock<HashMap<String, User>>>,
    roles: Arc<RwLock<HashMap<String, Role>>>,
    config: AuthConfig,
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthManager {
    pub fn new() -> Self {
        Self::new_with_config(AuthConfig::default())
    }

    pub fn new_with_config(config: AuthConfig) -> Self {
        let auth_manager = AuthManager {
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            config,
        };

        // Initialize with default postgres superuser
        let postgres_user = User {
            username: "postgres".to_string(),
            password_hash: "".to_string(), // Empty password for now
            roles: vec!["postgres".to_string()],
            is_superuser: true,
            can_login: true,
            connection_limit: None,
        };

        let postgres_role = Role {
            name: "postgres".to_string(),
            is_superuser: true,
            can_login: true,
            can_create_db: true,
            can_create_role: true,
            can_create_user: true,
            can_replication: true,
            grants: vec![Grant {
                permission: Permission::All,
                resource: ResourceType::All,
                granted_by: "system".to_string(),
                with_grant_option: true,
            }],
            inherited_roles: vec![],
        };

        // Add default users and roles
        let auth_manager_clone = AuthManager {
            users: auth_manager.users.clone(),
            roles: auth_manager.roles.clone(),
            config: auth_manager.config.clone(),
        };

        tokio::spawn({
            let users = auth_manager.users.clone();
            let roles = auth_manager.roles.clone();
            let auth_manager_spawn = auth_manager_clone;
            async move {
                users
                    .write()
                    .await
                    .insert("postgres".to_string(), postgres_user);
                roles
                    .write()
                    .await
                    .insert("postgres".to_string(), postgres_role);

                // Create predefined roles
                if let Err(e) = auth_manager_spawn.create_predefined_roles().await {
                    warn!("Failed to create predefined roles: {e:?}");
                }
            }
        });

        auth_manager
    }

    /// Add a new user to the system
    pub async fn add_user(&self, user: User) -> PgWireResult<()> {
        let mut users = self.users.write().await;
        users.insert(user.username.clone(), user);
        Ok(())
    }

    /// Add a new role to the system
    pub async fn add_role(&self, role: Role) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;
        roles.insert(role.name.clone(), role);
        Ok(())
    }

    /// Authenticate a user with username and password
    pub async fn authenticate(&self, username: &str, password: &str) -> PgWireResult<bool> {
        let users = self.users.read().await;

        if let Some(user) = users.get(username) {
            if !user.can_login {
                return Ok(false);
            }

            // Check password requirements based on configuration
            if self.config.require_passwords {
                // When passwords are required, reject empty passwords
                if password.is_empty() {
                    return Ok(false);
                }
                // Also require that the user has a non-empty password hash
                if user.password_hash.is_empty() {
                    return Ok(false);
                }
                // Check password match
                return Ok(password == user.password_hash);
            } else {
                // Legacy behavior: allow empty passwords if configured
                if user.password_hash.is_empty() {
                    return Ok(self.config.allow_empty_passwords || password.is_empty());
                }
                // Check password match for users with passwords
                return Ok(password == user.password_hash);
            }
        }

        // If user doesn't exist, reject
        Ok(false)
    }

    /// Get user information
    pub async fn get_user(&self, username: &str) -> Option<User> {
        let users = self.users.read().await;
        users.get(username).cloned()
    }

    /// Get role information
    pub async fn get_role(&self, role_name: &str) -> Option<Role> {
        let roles = self.roles.read().await;
        roles.get(role_name).cloned()
    }

    /// Check if user has a specific role
    pub async fn user_has_role(&self, username: &str, role_name: &str) -> bool {
        if let Some(user) = self.get_user(username).await {
            return user.roles.contains(&role_name.to_string()) || user.is_superuser;
        }
        false
    }

    /// List all users (for administrative purposes)
    pub async fn list_users(&self) -> Vec<String> {
        let users = self.users.read().await;
        users.keys().cloned().collect()
    }

    /// List all roles (for administrative purposes)
    pub async fn list_roles(&self) -> Vec<String> {
        let roles = self.roles.read().await;
        roles.keys().cloned().collect()
    }

    /// Grant permission to a role
    pub async fn grant_permission(
        &self,
        role_name: &str,
        permission: Permission,
        resource: ResourceType,
        granted_by: &str,
        with_grant_option: bool,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(role) = roles.get_mut(role_name) {
            let grant = Grant {
                permission,
                resource,
                granted_by: granted_by.to_string(),
                with_grant_option,
            };
            role.grants.push(grant);
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{role_name}\" does not exist"),
                ),
            )))
        }
    }

    /// Revoke permission from a role
    pub async fn revoke_permission(
        &self,
        role_name: &str,
        permission: Permission,
        resource: ResourceType,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(role) = roles.get_mut(role_name) {
            role.grants
                .retain(|grant| !(grant.permission == permission && grant.resource == resource));
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{role_name}\" does not exist"),
                ),
            )))
        }
    }

    /// Check if a user has a specific permission on a resource
    pub async fn check_permission(
        &self,
        username: &str,
        permission: Permission,
        resource: ResourceType,
    ) -> bool {
        // Superusers have all permissions
        if let Some(user) = self.get_user(username).await {
            if user.is_superuser {
                return true;
            }

            // Check permissions for each role the user has
            for role_name in &user.roles {
                if let Some(role) = self.get_role(role_name).await {
                    // Superuser role has all permissions
                    if role.is_superuser {
                        return true;
                    }

                    // Check direct grants
                    for grant in &role.grants {
                        if self.permission_matches(&grant.permission, &permission)
                            && self.resource_matches(&grant.resource, &resource)
                        {
                            return true;
                        }
                    }

                    // Check inherited roles recursively
                    for inherited_role in &role.inherited_roles {
                        if self
                            .check_role_permission(inherited_role, &permission, &resource)
                            .await
                        {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    /// Check if a role has a specific permission (helper for recursive checking)
    fn check_role_permission<'a>(
        &'a self,
        role_name: &'a str,
        permission: &'a Permission,
        resource: &'a ResourceType,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            if let Some(role) = self.get_role(role_name).await {
                if role.is_superuser {
                    return true;
                }

                // Check direct grants
                for grant in &role.grants {
                    if self.permission_matches(&grant.permission, permission)
                        && self.resource_matches(&grant.resource, resource)
                    {
                        return true;
                    }
                }

                // Check inherited roles
                for inherited_role in &role.inherited_roles {
                    if self
                        .check_role_permission(inherited_role, permission, resource)
                        .await
                    {
                        return true;
                    }
                }
            }

            false
        })
    }

    /// Check if a permission grant matches the requested permission
    fn permission_matches(&self, grant_permission: &Permission, requested: &Permission) -> bool {
        grant_permission == requested || matches!(grant_permission, Permission::All)
    }

    /// Check if a resource grant matches the requested resource
    fn resource_matches(&self, grant_resource: &ResourceType, requested: &ResourceType) -> bool {
        match (grant_resource, requested) {
            // Exact match
            (a, b) if a == b => true,
            // All resource type grants access to everything
            (ResourceType::All, _) => true,
            // Schema grants access to all tables in that schema
            (ResourceType::Schema(schema), ResourceType::Table(table)) => {
                // For simplicity, assume table names are schema.table format
                table.starts_with(&format!("{schema}."))
            }
            _ => false,
        }
    }

    /// Add role inheritance
    pub async fn add_role_inheritance(
        &self,
        child_role: &str,
        parent_role: &str,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(child) = roles.get_mut(child_role) {
            if !child.inherited_roles.contains(&parent_role.to_string()) {
                child.inherited_roles.push(parent_role.to_string());
            }
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{child_role}\" does not exist"),
                ),
            )))
        }
    }

    /// Remove role inheritance
    pub async fn remove_role_inheritance(
        &self,
        child_role: &str,
        parent_role: &str,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(child) = roles.get_mut(child_role) {
            child.inherited_roles.retain(|role| role != parent_role);
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{child_role}\" does not exist"),
                ),
            )))
        }
    }

    /// Create a new role with specific capabilities
    pub async fn create_role(&self, config: RoleConfig) -> PgWireResult<()> {
        let role = Role {
            name: config.name.clone(),
            is_superuser: config.is_superuser,
            can_login: config.can_login,
            can_create_db: config.can_create_db,
            can_create_role: config.can_create_role,
            can_create_user: config.can_create_user,
            can_replication: config.can_replication,
            grants: vec![],
            inherited_roles: vec![],
        };

        self.add_role(role).await
    }

    /// Create common predefined roles
    pub async fn create_predefined_roles(&self) -> PgWireResult<()> {
        // Read-only role
        self.create_role(RoleConfig {
            name: "readonly".to_string(),
            is_superuser: false,
            can_login: false,
            can_create_db: false,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
        })
        .await?;

        self.grant_permission(
            "readonly",
            Permission::Select,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        // Read-write role
        self.create_role(RoleConfig {
            name: "readwrite".to_string(),
            is_superuser: false,
            can_login: false,
            can_create_db: false,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
        })
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Select,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Insert,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Update,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Delete,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        // Database admin role
        self.create_role(RoleConfig {
            name: "dbadmin".to_string(),
            is_superuser: false,
            can_login: true,
            can_create_db: true,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
        })
        .await?;

        self.grant_permission(
            "dbadmin",
            Permission::All,
            ResourceType::All,
            "system",
            true,
        )
        .await?;

        Ok(())
    }

    /// Get the current authentication configuration
    pub fn get_config(&self) -> &AuthConfig {
        &self.config
    }

    /// Enable password requirements for all users
    pub fn require_passwords(&mut self) {
        self.config.require_passwords = true;
        self.config.allow_empty_passwords = false;
    }

    /// Disable password requirements (allow passwordless login)
    pub fn allow_passwordless_login(&mut self) {
        self.config.require_passwords = false;
        self.config.allow_empty_passwords = true;
    }

    /// Set whether empty passwords are allowed when password requirements are disabled
    pub fn set_allow_empty_passwords(&mut self, allow: bool) {
        if !self.config.require_passwords {
            self.config.allow_empty_passwords = allow;
        }
    }

    /// Wait for initialization to complete (postgres user to be created)
    pub async fn wait_for_initialization(&self) {
        // Wait for postgres user to be created
        let max_wait = 100; // 100 * 10ms = 1 second max wait
        for _ in 0..max_wait {
            let users = self.users.read().await;
            if users.contains_key("postgres") {
                return;
            }
            drop(users);
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Update the postgres user to require a password
    pub async fn set_postgres_password(&self, password: &str) -> PgWireResult<()> {
        // Wait for initialization to complete
        self.wait_for_initialization().await;
        
        let mut users = self.users.write().await;
        if let Some(postgres_user) = users.get_mut("postgres") {
            postgres_user.password_hash = password.to_string();
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(),
                    "postgres user not found".to_string(),
                ),
            )))
        }
    }
}


// Password authentication is implemented using pgwire handlers.
// See handlers.rs UnifiedStartupHandler for the actual implementation.

/// Simple AuthSource implementation that accepts any user with empty password
pub struct SimpleAuthSource {
    auth_manager: Arc<AuthManager>,
}

impl SimpleAuthSource {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        SimpleAuthSource { auth_manager }
    }
}

#[async_trait]
impl AuthSource for SimpleAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("anonymous");

        // Wait for initialization to complete
        self.auth_manager.wait_for_initialization().await;

        // Check if user exists and can login
        if let Some(user) = self.auth_manager.get_user(username).await {
            if user.can_login {
                let config = self.auth_manager.get_config();
                
                // If password requirements are enforced, user must have a password
                if config.require_passwords {
                    if user.password_hash.is_empty() {
                        // User has no password but passwords are required
                        return Err(PgWireError::UserError(Box::new(
                            pgwire::error::ErrorInfo::new(
                                "FATAL".to_string(),
                                "28P01".to_string(), // invalid_password
                                format!("User \"{username}\" requires a password"),
                            ),
                        )));
                    }
                    // Return the user's password hash for verification
                    return Ok(Password::new(None, user.password_hash.into_bytes()));
                } else {
                    // Legacy mode: allow empty passwords based on configuration
                    if user.password_hash.is_empty() {
                        if config.allow_empty_passwords {
                            return Ok(Password::new(None, vec![]));
                        } else {
                            return Err(PgWireError::UserError(Box::new(
                                pgwire::error::ErrorInfo::new(
                                    "FATAL".to_string(),
                                    "28P01".to_string(), // invalid_password
                                    format!("Empty passwords not allowed for user \"{username}\""),
                                ),
                            )));
                        }
                    } else {
                        // User has a password, return it for verification
                        return Ok(Password::new(None, user.password_hash.into_bytes()));
                    }
                }
            }
        }

        // User not found or cannot login
        Err(PgWireError::UserError(Box::new(
            pgwire::error::ErrorInfo::new(
                "FATAL".to_string(),
                "28P01".to_string(), // invalid_password
                format!("password authentication failed for user \"{username}\""),
            ),
        )))
    }
}

/// Helper function to create auth source with auth manager
pub fn create_auth_source(auth_manager: Arc<AuthManager>) -> SimpleAuthSource {
    SimpleAuthSource::new(auth_manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auth_manager_creation() {
        let auth_manager = AuthManager::new();

        // Wait a bit for the default user to be added
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let users = auth_manager.list_users().await;
        assert!(users.contains(&"postgres".to_string()));
    }

    #[tokio::test]
    async fn test_user_authentication() {
        let auth_manager = AuthManager::new();

        // Wait for initialization
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Test postgres user authentication
        assert!(auth_manager.authenticate("postgres", "").await.unwrap());
        assert!(!auth_manager
            .authenticate("nonexistent", "password")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_role_management() {
        let auth_manager = AuthManager::new();

        // Wait for initialization
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Test role checking
        assert!(auth_manager.user_has_role("postgres", "postgres").await);
        assert!(auth_manager.user_has_role("postgres", "any_role").await); // superuser
    }
}
