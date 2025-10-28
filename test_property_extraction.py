#!/usr/bin/env python3

import sys
import os
import time
import json
import random
import threading
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import functions from enhanced_99acres.py
sys.path.append('.')
from enhanced_99acres import raw_scrapper, askForCaptcha, stop_event

def test_single_url():
    """Test property extraction with a single URL"""
    
    # Test URL from your data
    test_url = "https://www.99acres.com/niranjan-tridha-kudlu-gate-bangalore-south-npxid-r37246"
    
    print(f"🧪 Testing property extraction with: {test_url}")
    print("="*80)
    
    try:
        result = raw_scrapper(test_url)
        
        if result:
            print(f"\n✅ Successfully scraped data from: {test_url}")
            print(f"📊 Result structure:")
            print(f"  - URL: {result.get('url', 'N/A')}")
            print(f"  - Basic Details: {'✅' if result.get('raw_data', {}).get('basicDetails') else '❌'}")
            print(f"  - Components: {'✅' if result.get('raw_data', {}).get('components') else '❌'}")
            
            # Check property listings
            property_listings = result.get('property_listings', {})
            rental_props = property_listings.get('rental_properties', [])
            resale_props = property_listings.get('resale_properties', [])
            
            print(f"\n🏠 Property Listings:")
            print(f"  - Rental Properties: {len(rental_props)}")
            print(f"  - Resale Properties: {len(resale_props)}")
            
            # Show sample properties
            if rental_props:
                print(f"\n📋 Sample Rental Property:")
                sample_rental = rental_props[0]
                for key, value in sample_rental.items():
                    print(f"    {key}: {value}")
            
            if resale_props:
                print(f"\n📋 Sample Resale Property:")
                sample_resale = resale_props[0]
                for key, value in sample_resale.items():
                    print(f"    {key}: {value}")
            
            # Show extraction summary
            extraction_summary = result.get('extraction_summary', {})
            print(f"\n📈 Extraction Summary:")
            for key, value in extraction_summary.items():
                if key != 'extraction_errors':
                    print(f"  {key}: {value}")
            
            if extraction_summary.get('extraction_errors'):
                print(f"  Errors: {len(extraction_summary['extraction_errors'])}")
                for error in extraction_summary['extraction_errors'][:3]:
                    print(f"    - {error}")
            
            # Save test result
            with open('test_result.json', 'w') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print(f"\n💾 Test result saved to test_result.json")
            
        else:
            print(f"\n❌ Failed to scrape data from: {test_url}")
            
    except Exception as e:
        print(f"\n❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_url()