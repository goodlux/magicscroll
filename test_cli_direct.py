#!/usr/bin/env python3
"""Quick test of the CLI to see what error we get."""

import sys
import os
import subprocess

# Change to the magicscroll directory
os.chdir('/Users/rob/repos/magicscroll')

print("🧪 Testing CLI directly...")
print("Working directory:", os.getcwd())

try:
    # Run the CLI script directly 
    result = subprocess.run([
        sys.executable, '-m', 'magicscroll.cli'
    ], capture_output=True, text=True, timeout=10)
    
    print("STDOUT:")
    print(result.stdout)
    
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    print(f"Return code: {result.returncode}")
    
except subprocess.TimeoutExpired:
    print("CLI started successfully (timed out waiting for input - this is good!)")
except Exception as e:
    print(f"Error running CLI: {e}")

print("\n" + "="*50)
print("Also testing debug script...")

try:
    result = subprocess.run([
        sys.executable, 'debug_cli.py'
    ], capture_output=True, text=True, timeout=30)
    
    print("Debug script STDOUT:")
    print(result.stdout)
    
    if result.stderr:
        print("Debug script STDERR:")
        print(result.stderr)
    
    print(f"Debug script return code: {result.returncode}")
    
except Exception as e:
    print(f"Error running debug script: {e}")
