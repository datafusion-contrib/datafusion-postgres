#!/usr/bin/env bash

set -e

echo "🔐 Testing Secure Authentication Example"
echo "========================================"

# Build the example
echo "Building secure auth example..."
cd datafusion-postgres
cargo build --example secure_auth_server

echo ""
echo "✅ Secure auth example built successfully!"
echo ""
echo "🚀 To run the secure auth server (requires passwords):"
echo "   cargo run --example secure_auth_server --manifest-path datafusion-postgres/Cargo.toml"
echo ""
echo "🔐 Authentication will be REQUIRED for all connections:"
echo "   psql -h 127.0.0.1 -p 5440 -U postgres         # Password: secure_postgres_password"
echo "   psql -h 127.0.0.1 -p 5440 -U admin            # Password: admin_secure_pass"
echo "   psql -h 127.0.0.1 -p 5440 -U reader           # Password: reader_secure_pass"
echo ""
echo "❌ These will be REJECTED:"
echo "   psql -h 127.0.0.1 -p 5440 -U postgres         # No password (will fail)"
echo "   psql -h 127.0.0.1 -p 5440 -U admin            # Wrong password (will fail)"
echo ""
echo "🧪 Test password enforcement:"
echo "   python3 tests-integration/test_password_enforcement.py"