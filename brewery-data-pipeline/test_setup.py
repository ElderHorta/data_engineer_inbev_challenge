#!/usr/bin/env python3
"""
Quick Setup Test Script

Tests basic imports and configuration without Docker.
Run this to verify your environment is correctly set up.

Usage:
    python test_setup.py
"""

import sys
import os

def test_python_version():
    """Check Python version."""
    print("=" * 60)
    print("Testing Python Version")
    print("=" * 60)
    version = sys.version_info
    print(f"Python {version.major}.{version.minor}.{version.micro}")
    
    if version.major >= 3 and version.minor >= 10:
        print("✓ Python version OK (3.10+)")
        return True
    else:
        print("✗ Python version too old (need 3.10+)")
        return False


def test_imports():
    """Test critical imports."""
    print("\n" + "=" * 60)
    print("Testing Package Imports")
    print("=" * 60)
    
    packages = [
        ("yaml", "PyYAML"),
        ("requests", "requests"),
        ("dotenv", "python-dotenv"),
    ]
    
    all_ok = True
    for module_name, package_name in packages:
        try:
            __import__(module_name)
            print(f"✓ {package_name:20s} - OK")
        except ImportError:
            print(f"✗ {package_name:20s} - NOT INSTALLED")
            all_ok = False
    
    return all_ok


def test_env_file():
    """Check .env file."""
    print("\n" + "=" * 60)
    print("Testing Environment Configuration")
    print("=" * 60)
    
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    
    if os.path.exists(env_path):
        print(f"✓ .env file exists at: {env_path}")
        return True
    else:
        print(f"✗ .env file NOT FOUND at: {env_path}")
        print(f"  Run: copy .env.example .env")
        return False


def test_config_loading():
    """Test configuration loading."""
    print("\n" + "=" * 60)
    print("Testing Configuration Loading")
    print("=" * 60)
    
    try:
        # Add src to path
        src_path = os.path.join(os.path.dirname(__file__), 'src')
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        
        from utils.config import load_config, get_default_config
        
        try:
            config = load_config()
            print("✓ Configuration loaded from YAML")
            print(f"  Storage type: {config.get('storage', {}).get('type', 'unknown')}")
            print(f"  Base path: {config.get('storage', {}).get('base_path', 'unknown')}")
        except FileNotFoundError:
            print("⚠ YAML config not found, using defaults")
            config = get_default_config()
        
        return True
    except Exception as e:
        print(f"✗ Configuration loading failed: {e}")
        return False


def test_api_connectivity():
    """Test API connection."""
    print("\n" + "=" * 60)
    print("Testing API Connectivity")
    print("=" * 60)
    
    try:
        import requests
        
        api_url = "https://api.openbrewerydb.org/v1/breweries"
        print(f"Testing connection to: {api_url}")
        
        response = requests.get(api_url, params={"per_page": 1}, timeout=10)
        
        if response.status_code == 200:
            print(f"✓ API connection OK (status {response.status_code})")
            data = response.json()
            if data:
                print(f"  Sample brewery: {data[0].get('name', 'Unknown')}")
            return True
        else:
            print(f"✗ API returned status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"✗ API connection failed: {e}")
        return False


def test_docker():
    """Test Docker availability."""
    print("\n" + "=" * 60)
    print("Testing Docker")
    print("=" * 60)
    
    import subprocess
    
    try:
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, 
                              text=True, 
                              timeout=5)
        if result.returncode == 0:
            print(f"✓ Docker installed: {result.stdout.strip()}")
            
            # Test Docker daemon
            result = subprocess.run(['docker', 'ps'], 
                                  capture_output=True, 
                                  text=True, 
                                  timeout=5)
            if result.returncode == 0:
                print("✓ Docker daemon is running")
                return True
            else:
                print("✗ Docker daemon is not running")
                print("  Please start Docker Desktop")
                return False
        else:
            print("✗ Docker command failed")
            return False
    except FileNotFoundError:
        print("✗ Docker NOT installed")
        print("\nTo install Docker:")
        print("  1. Download Docker Desktop from: https://www.docker.com/products/docker-desktop")
        print("  2. Install and restart your computer")
        print("  3. Start Docker Desktop")
        return False
    except Exception as e:
        print(f"✗ Docker check failed: {e}")
        return False


def main():
    """Run all tests."""
    print("\n")
    print("*" * 60)
    print(" BREWERY DATA PIPELINE - SETUP TEST")
    print("*" * 60)
    print()
    
    results = {
        "Python Version": test_python_version(),
        "Package Imports": test_imports(),
        "Environment File": test_env_file(),
        "Configuration": test_config_loading(),
        "API Connectivity": test_api_connectivity(),
        "Docker": test_docker(),
    }
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{test_name:20s}: {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL TESTS PASSED!")
        print("\nYou're ready to run:")
        print("  docker-compose build")
        print("  docker-compose up -d")
    else:
        print("⚠ SOME TESTS FAILED")
        
        if not results["Docker"]:
            print("\n⚠ Docker is required to run the full pipeline")
            print("  However, you can still run unit tests locally")
            
        if not results["Package Imports"]:
            print("\nTo install missing packages:")
            print("  pip install -r requirements.txt")
    
    print("=" * 60)
    print()
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
