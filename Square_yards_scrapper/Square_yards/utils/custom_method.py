import os
import json
from bs4 import BeautifulSoup
import requests
def load_json(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path,'r') as f:
                return json.load(f)
        except Exception:
                return []
    return []

def save_json(file_path,record):
    try:
        with open(file_path,'w') as f:
            json.dump(record,f,indent=2)
    except Exception:
        print(f'failed to save the record at :{file_path}')

def save_html(file_path,response):    
    try:
        with open(file_path,'w') as f:
            f.write(str(response))
    except Exception:
        print(f'failed to save the html record at :{file_path}')
          
def scraper(response):     
    return BeautifulSoup(response.text,'html.parser')

def fetch_page(url):
    if not url:
        return None    
    try:         
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            # "Accept-Encoding": "gzip, deflate, br", 
        }
        response = requests.get(url,headers=headers)
        response.raise_for_status()
        # response.encoding = "utf-8"
        return response
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
    return None