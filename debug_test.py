#!/usr/bin/env python3
"""
Debug test to understand the exact issue
"""

import psycopg
import subprocess
import time
import os
import signal
import sys

def debug_connection():
    """Debug the connection and query issue"""
    print("🔍 Debug Test for Password Enforcement")
    print("=====================================")
    
    server_process = None
    try:
        print("\n🚀 Starting secure auth server...")
        
        # Start the secure server
        server_process = subprocess.Popen(
            ["cargo", "run", "--example", "secure_auth_server"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
            cwd="datafusion-postgres"
        )
        
        # Wait for server to start
        print("  ⏳ Waiting for server to start...")
        time.sleep(8)  # Wait longer
        
        # Check if process is still running
        if server_process.poll() is not None:
            stdout, stderr = server_process.communicate()
            print(f"❌ Server process exited early")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return False
        
        print("✅ Server process is running")
        
        print("\n🔌 Testing connection...")
        try:
            # Test connection
            print("  Attempting connection...")
            conn = psycopg.connect("host=127.0.0.1 port=5440 user=postgres password=secure_postgres_password", connect_timeout=10)
            print("  ✅ Connection established!")
            
            with conn:
                with conn.cursor() as cur:
                    print("  📋 Testing SHOW server_version...")
                    try:
                        cur.execute("SHOW server_version")
                        print("  Query executed successfully")
                        
                        result = cur.fetchone()
                        print(f"  Result: {result}")
                        print(f"  Result type: {type(result)}")
                        
                        if result is None:
                            print("  ❌ Query returned None")
                        elif len(result) == 0:
                            print("  ❌ Query returned empty tuple")
                        else:
                            print(f"  ✅ Query returned: {result[0]}")
                            
                    except Exception as query_error:
                        print(f"  ❌ Query execution failed: {query_error}")
                        import traceback
                        traceback.print_exc()
                    
                    print("  📋 Testing other queries...")
                    try:
                        cur.execute("SELECT 'test' as test_column")
                        result = cur.fetchone()
                        print(f"  SELECT test result: {result}")
                    except Exception as e:
                        print(f"  SELECT test failed: {e}")
            
            conn.close()
            print("  Connection closed")
            
        except Exception as conn_error:
            print(f"  ❌ Connection failed: {conn_error}")
            import traceback
            traceback.print_exc()
            return False
        
        print("\n✅ Debug test completed")
        return True
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Clean up server process
        if server_process:
            try:
                # Kill the entire process group
                os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
                time.sleep(2)
                if server_process.poll() is None:
                    os.killpg(os.getpgid(server_process.pid), signal.SIGKILL)
            except:
                pass

if __name__ == "__main__":
    success = debug_connection()
    sys.exit(0 if success else 1)