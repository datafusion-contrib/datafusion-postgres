#!/usr/bin/env python3
"""
Test to verify if wrong passwords are actually rejected
"""

import psycopg
import subprocess
import time
import os
import signal
import sys

def test_wrong_password():
    """Test if wrong passwords are rejected"""
    print("🔍 Testing Wrong Password Rejection")
    print("===================================")
    
    server_process = None
    try:
        print("\n🚀 Starting secure auth server...")
        
        # Start the secure server
        server_process = subprocess.Popen(
            ["cargo", "run", "--example", "secure_auth_server"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
            cwd="../datafusion-postgres"
        )
        
        # Wait for server to start
        time.sleep(8)
        
        if server_process.poll() is not None:
            print("❌ Server failed to start")
            return False
        
        print("✅ Server started")
        
        print("\n🔑 Test 1: Correct password (should work)")
        try:
            with psycopg.connect("host=127.0.0.1 port=5440 user=postgres password=secure_postgres_password") as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW server_version")
                    version = cur.fetchone()[0]
                    print(f"  ✅ Correct password works: {version}")
        except Exception as e:
            print(f"  ❌ Correct password failed: {e}")
            return False
        
        print("\n🔑 Test 2: Wrong password (should be rejected)")
        try:
            with psycopg.connect("host=127.0.0.1 port=5440 user=postgres password=WRONG_PASSWORD") as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW server_version")
                    version = cur.fetchone()[0]
                    print(f"  ❌ SECURITY ISSUE: Wrong password was accepted! {version}")
                    return False  # This is bad - wrong password should not work
        except Exception as e:
            print(f"  ✅ Wrong password correctly rejected: {e}")
        
        print("\n🔑 Test 3: No password (should be rejected)")
        try:
            with psycopg.connect("host=127.0.0.1 port=5440 user=postgres") as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW server_version")
                    version = cur.fetchone()[0]
                    print(f"  ❌ SECURITY ISSUE: No password was accepted! {version}")
                    return False  # This is bad - no password should not work
        except Exception as e:
            print(f"  ✅ No password correctly rejected: {e}")
        
        print("\n📊 Password Security Test Results:")
        print("  ✅ Correct password works")
        print("  ✅ Wrong password rejected")  
        print("  ✅ No password rejected")
        print("  🔒 TRUE password enforcement implemented")
        return True
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
        
    finally:
        if server_process:
            try:
                os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
                time.sleep(2)
                if server_process.poll() is None:
                    os.killpg(os.getpgid(server_process.pid), signal.SIGKILL)
            except:
                pass

if __name__ == "__main__":
    success = test_wrong_password()
    if not success:
        print("\n⚠️  CONCLUSION: Password enforcement is NOT fully implemented.")
        print("    The current implementation only validates user existence and password hash presence.")
        print("    Actual password validation still requires pgwire authentication handlers.")
    else:
        print("\n🎉 CONCLUSION: Full password enforcement is implemented!")
    sys.exit(0 if success else 1)