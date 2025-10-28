# Enhanced 99acres Scraper - Property Listings Extraction

## Overview

The Enhanced 99acres Scraper is a comprehensive web scraping solution that extracts both project-level data and individual property listings from 99acres.com. This enhanced version includes robust property extraction capabilities, data validation, error handling, and performance monitoring.

## 🚀 Key Features

### 1. **Comprehensive Property Extraction**
- Extract rental and resale property listings from project pages
- Parse individual property details (unit type, size, price, posted date, etc.)
- Support for both rental and sale property types
- Automatic URL construction from seoUrl fields

### 2. **Data Validation & Formatting**
- Validate extracted property data for required fields
- Format data into standardized JSON structure
- Clean and normalize text fields
- Handle missing or incomplete property information with defaults

### 3. **Robust Error Handling**
- Retry logic with exponential backoff for failed extractions
- Rate limiting to comply with website policies
- Comprehensive error logging and tracking
- Partial result handling when some properties fail

### 4. **Performance Monitoring**
- Track extraction success rates and timing
- Monitor rate limiting compliance
- Detailed performance metrics per project
- Comprehensive logging for debugging and optimization

## 📁 File Structure

```
├── enhanced_99acres.py          # Main scraper with property extraction
├── example_usage.py             # Comprehensive usage examples
├── README_PROPERTY_EXTRACTION.md # This documentation file
└── bangalore/
    ├── raw_bangalore_99acres.ndjson      # Main output file
    ├── sample_raw_bangalore_99acres.ndjson # Sample data
    ├── unique_urls_sqy_99acres.json      # Input URLs
    └── analyze_data.py                   # Data analysis script
```

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.7+
- Required packages: `requests`, `beautifulsoup4`, `ijson`, `urllib3`

### Installation
```bash
# Install required packages
pip install requests beautifulsoup4 ijson urllib3

# Ensure you have the input URLs file
# bangalore/unique_urls_sqy_99acres.json should contain project URLs
```

## 📊 Data Structure

### Input: Project Components Data
```json
{
  "rentalProperties": {
    "data": [
      {
        "seoUrl": "/4-bhk-apartment-for-rent-...",
        "unitType": "4 BHK",
        "area": "4500 sqft",
        "rent": "Rs. 65,000/month"
      }
    ]
  },
  "resaleProperties": {
    "data": [
      {
        "seoUrl": "/4-bhk-apartment-for-sale-...",
        "unitType": "4 BHK",
        "area": "4500 sqft",
        "price": "Rs. 2.8 Cr"
      }
    ]
  }
}
```

### Output: Enhanced Property Data
```json
{
  "url": "https://www.99acres.com/project-url",
  "raw_data": {
    "basicDetails": { /* project information */ },
    "components": { /* rental/resale data */ }
  },
  "property_listings": {
    "rental_properties": [
      {
        "property_url": "https://www.99acres.com/rental-property-url",
        "unit_type": "4 BHK Apartment",
        "size": "4500 sqft",
        "price": "Rs. 65,000/month",
        "type": "rent",
        "posted_date": "2025-01-20",
        "posted_by": "Owner",
        "source_project_url": "https://www.99acres.com/project-url",
        "extraction_timestamp": "2025-01-23T10:30:15Z"
      }
    ],
    "resale_properties": [ /* similar structure */ ]
  },
  "extraction_summary": {
    "total_rental_found": 2,
    "total_resale_found": 2,
    "successful_rental_extractions": 2,
    "successful_resale_extractions": 2,
    "failed_extractions": 0,
    "extraction_errors": []
  },
  "performance_metrics": {
    "total_extraction_time": 8.5,
    "success_rate": 100.0,
    "average_time_per_property": 2.125,
    "properties_per_minute": 28.2
  }
}
```

## 🚀 Usage Examples

### Basic Usage
```python
from enhanced_99acres import raw_scrapper

# Scrape a project with property listings
project_url = "https://www.99acres.com/your-project-url"
result = raw_scrapper(project_url)

if result:
    rental_properties = result['property_listings']['rental_properties']
    resale_properties = result['property_listings']['resale_properties']
    
    print(f"Found {len(rental_properties)} rental properties")
    print(f"Found {len(resale_properties)} resale properties")
```

### Extract Property URLs Only
```python
from enhanced_99acres import extract_property_urls

components = {
    "rentalProperties": {"data": [{"seoUrl": "/rental-url"}]},
    "resaleProperties": {"data": [{"seoUrl": "/resale-url"}]}
}

property_urls = extract_property_urls(components)
print(f"Rental URLs: {property_urls['rental_urls']}")
print(f"Resale URLs: {property_urls['resale_urls']}")
```

### Fetch Individual Property Data
```python
from enhanced_99acres import fetch_property_data

property_url = "https://www.99acres.com/property-url"
property_data = fetch_property_data(property_url, "rent")

if property_data:
    print(f"Unit Type: {property_data['unit_type']}")
    print(f"Price: {property_data['price']}")
    print(f"Size: {property_data['size']}")
```

## 🔧 Configuration

### Rate Limiting
The scraper includes built-in rate limiting to respect website policies:
- 1-2 second delays between property requests
- Exponential backoff for retries
- Rate limiting compliance monitoring

### Error Handling
- Maximum 2 retry attempts per property
- Comprehensive error logging
- Partial result handling
- Graceful degradation on failures

### Performance Tuning
```python
# In enhanced_99acres.py, you can adjust:
MAX_WORKERS = 2  # Number of concurrent threads
save_every = 10  # Save progress every N records
```

## 📈 Monitoring & Analysis

### View Extraction Results
```bash
# View the output file
cat bangalore/raw_bangalore_99acres.ndjson

# Count total records
wc -l bangalore/raw_bangalore_99acres.ndjson

# View recent records
tail -n 5 bangalore/raw_bangalore_99acres.ndjson
```

### Analyze Data
```bash
# Run the analysis script
cd bangalore
python analyze_data.py
```

### Example Analysis Output
```
📊 PROJECT ANALYSIS
Total Projects: 3
Top Builders:
  Prestige Group: 1 projects
  Brigade Group: 1 projects
  Sobha Limited: 1 projects

🏠 PROPERTY ANALYSIS
Total Rental Properties: 5
Total Resale Properties: 7
Total Properties: 12

⚡ EXTRACTION PERFORMANCE
Overall Success Rate: 100.0%
Total Extraction Time: 27.6 seconds
Average Time per Project: 9.2 seconds
```

## 🎯 Key Functions

### Property Extraction Functions
- `extract_property_urls(components_data)` - Extract property URLs from components
- `parse_rental_properties(components)` - Parse rental property data
- `parse_resale_properties(components)` - Parse resale property data
- `construct_property_url(seo_url)` - Construct full URLs

### Data Processing Functions
- `fetch_property_data(property_url, property_type)` - Fetch individual property data
- `validate_property_data(property_data)` - Validate extracted data
- `format_property_data(raw_data, property_type, source_url)` - Format data
- `handle_missing_property_information(property_data)` - Handle missing data

### Error Handling Functions
- `retry_property_extraction(property_url, property_type, max_retries)` - Retry logic
- `apply_property_rate_limiting()` - Rate limiting
- `log_extraction_error(error_message, property_url, errors_list)` - Error logging

### Monitoring Functions
- `track_extraction_performance_metrics(summary, start_time)` - Performance tracking
- `monitor_rate_limiting_compliance(request_times)` - Rate limit monitoring
- `log_property_extraction_summary(project_url, summary, metrics)` - Comprehensive logging

## 🚨 Important Notes

### Rate Limiting Compliance
- The scraper includes built-in delays to respect website policies
- Monitor the rate limiting compliance logs
- Adjust delays if needed for your use case

### Error Handling
- The scraper continues processing even if some properties fail
- Check extraction_errors in the output for failed extractions
- Partial results are returned when some properties fail

### Data Quality
- All extracted data is validated and formatted
- Missing fields are handled with default values
- Timestamps are added for tracking

## 🔍 Troubleshooting

### Common Issues

1. **No Properties Found**
   - Check if the project URL contains components section
   - Verify the components data structure matches expected format

2. **High Failure Rate**
   - Check network connectivity
   - Verify URLs are accessible
   - Review error logs for specific issues

3. **Rate Limiting Issues**
   - Increase delays between requests
   - Monitor compliance logs
   - Consider reducing concurrent workers

### Debug Mode
Enable detailed logging by checking the console output during scraping. The scraper provides comprehensive logging for:
- Successful extractions
- Failed extractions with reasons
- Performance metrics
- Rate limiting compliance

## 📝 Version History

### Version 2.0 (Current)
- ✅ Property listings extraction
- ✅ Data validation and formatting
- ✅ Rate limiting and error recovery
- ✅ Comprehensive logging and monitoring
- ✅ Performance metrics tracking

### Version 1.0 (Previous)
- Basic project data extraction
- Simple error handling
- Basic output format

## 🤝 Contributing

To contribute to the enhanced scraper:
1. Test new features thoroughly
2. Maintain backward compatibility
3. Add comprehensive logging
4. Update documentation
5. Follow existing code patterns

## 📞 Support

For issues or questions:
1. Check the troubleshooting section
2. Review the example usage
3. Examine the comprehensive logs
4. Test with sample data first

---

**Happy Scraping! 🏠📊**