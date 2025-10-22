#!/usr/bin/env python3

import requests
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
]

def create_session():
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1,
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,
        pool_maxsize=10,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def test_fetch(url):
    session = create_session()
    
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }

        print(f"Testing URL: {url}")
        response = session.get(
            url, 
            headers=headers, 
            timeout=(15, 30),  # (connect_timeout, read_timeout)
            allow_redirects=True
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Response URL: {response.url}")
        print(f"Content Length: {len(response.text)}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            scripts = soup.find_all("script")
            print(f"Found {len(scripts)} script tags")
            
            for script in scripts:
                if script.string and "window.__initialData__" in script.string:
                    print("✅ Found window.__initialData__!")
                    return True
            
            print("❌ No window.__initialData__ found")
            return False
        else:
            print(f"❌ Failed with status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        session.close()

if __name__ == "__main__":
    test_url = "https://www.99acres.com/niranjan-tridha-kudlu-gate-bangalore-south-npxid-r37246"
    test_fetch(test_url)