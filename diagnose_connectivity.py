#!/usr/bin/env python3

import requests
import socket
import time
from urllib.parse import urlparse

def test_dns_resolution(hostname):
    """Test if we can resolve the hostname"""
    try:
        ip = socket.gethostbyname(hostname)
        print(f"✅ DNS Resolution: {hostname} -> {ip}")
        return True
    except socket.gaierror as e:
        print(f"❌ DNS Resolution failed: {e}")
        return False

def test_tcp_connection(hostname, port=443):
    """Test if we can establish a TCP connection"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((hostname, port))
        sock.close()
        if result == 0:
            print(f"✅ TCP Connection: {hostname}:{port}")
            return True
        else:
            print(f"❌ TCP Connection failed: {hostname}:{port} (error code: {result})")
            return False
    except Exception as e:
        print(f"❌ TCP Connection error: {e}")
        return False

def test_http_request(url):
    """Test HTTP request with different configurations"""
    print(f"\n🔍 Testing HTTP request to: {url}")
    
    configs = [
        {"name": "Default", "kwargs": {}},
        {"name": "No SSL Verify", "kwargs": {"verify": False}},
        {"name": "HTTP/1.1", "kwargs": {"headers": {"Connection": "close"}}},
        {"name": "Long timeout", "kwargs": {"timeout": 60}},
    ]
    
    for config in configs:
        try:
            print(f"  Testing {config['name']}...")
            response = requests.get(url, timeout=10, **config['kwargs'])
            print(f"  ✅ {config['name']}: Status {response.status_code}, Length: {len(response.text)}")
            return response
        except requests.exceptions.Timeout:
            print(f"  ❌ {config['name']}: Timeout")
        except requests.exceptions.ConnectionError as e:
            print(f"  ❌ {config['name']}: Connection Error - {e}")
        except requests.exceptions.SSLError as e:
            print(f"  ❌ {config['name']}: SSL Error - {e}")
        except Exception as e:
            print(f"  ❌ {config['name']}: Other Error - {e}")
    
    return None

def main():
    test_url = "https://www.99acres.com/"
    parsed = urlparse(test_url)
    hostname = parsed.hostname
    
    print("🔍 Connectivity Diagnosis for 99acres.com")
    print("=" * 50)
    
    # Test DNS resolution
    if not test_dns_resolution(hostname):
        print("❌ Cannot resolve hostname. Check your DNS settings.")
        return
    
    # Test TCP connection
    if not test_tcp_connection(hostname, 443):
        print("❌ Cannot establish TCP connection. Site might be blocked or down.")
        return
    
    # Test HTTP requests
    response = test_http_request(test_url)
    
    if response:
        print(f"\n✅ Successfully connected to {test_url}")
        print(f"Final URL: {response.url}")
        print(f"Status: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
    else:
        print(f"\n❌ All HTTP request methods failed for {test_url}")
        print("This suggests the site is blocking requests or there's a network issue.")

if __name__ == "__main__":
    main()