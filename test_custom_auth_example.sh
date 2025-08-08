#!/usr/bin/env bash

set -e

echo "🔑 Testing Custom Authentication Example"
echo "========================================"

# Build the example
echo "Building custom auth example..."
cd datafusion-postgres
cargo build --example custom_auth_server

echo ""
echo "✅ Custom auth example built successfully!"
echo ""
echo "🚀 To run the custom auth server:"
echo "   cargo run --example custom_auth_server --manifest-path datafusion-postgres/Cargo.toml"
echo ""
echo "📝 Then connect using:"
echo "   psql -h 127.0.0.1 -p 5439 -U postgres         # Default user (no password)"
echo "   psql -h 127.0.0.1 -p 5439 -U admin            # Admin user (password: admin_password)"
echo "   psql -h 127.0.0.1 -p 5439 -U reader           # Reader user (password: reader_password)"
echo ""
echo "🧪 Or run a quick test:"
echo "   python3 -c \"import psycopg; conn = psycopg.connect('host=127.0.0.1 port=5439 user=postgres'); print('Connected successfully!')\""