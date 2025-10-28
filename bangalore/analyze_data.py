#!/usr/bin/env python3
"""
Script to analyze the enhanced 99acres scraper data.
This script helps you understand and explore the scraped property data.
"""

import json
import sys
import os
from collections import defaultdict, Counter

def load_ndjson(file_path):
    """Load NDJSON file and return list of records."""
    records = []
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return records
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error on line {line_num}: {e}")
                        continue
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    
    return records

def analyze_projects(records):
    """Analyze project-level data."""
    print("📊 PROJECT ANALYSIS")
    print("=" * 50)
    
    total_projects = len(records)
    print(f"Total Projects: {total_projects}")
    
    if total_projects == 0:
        return
    
    # Analyze builders
    builders = Counter()
    locations = Counter()
    project_types = Counter()
    
    for record in records:
        basic_details = record.get('raw_data', {}).get('basicDetails', {})
        builders[basic_details.get('builderName', 'Unknown')] += 1
        locations[basic_details.get('location', 'Unknown')] += 1
        project_types[basic_details.get('projectType', 'Unknown')] += 1
    
    print(f"\nTop Builders:")
    for builder, count in builders.most_common(5):
        print(f"  {builder}: {count} projects")
    
    print(f"\nTop Locations:")
    for location, count in locations.most_common(5):
        print(f"  {location}: {count} projects")
    
    print(f"\nProject Types:")
    for ptype, count in project_types.most_common():
        print(f"  {ptype}: {count} projects")

def analyze_properties(records):
    """Analyze property-level data."""
    print("\n🏠 PROPERTY ANALYSIS")
    print("=" * 50)
    
    total_rental = 0
    total_resale = 0
    unit_types = Counter()
    posted_by = Counter()
    
    for record in records:
        property_listings = record.get('property_listings', {})
        
        # Rental properties
        rental_props = property_listings.get('rental_properties', [])
        total_rental += len(rental_props)
        
        for prop in rental_props:
            unit_types[prop.get('unit_type', 'Unknown')] += 1
            posted_by[prop.get('posted_by', 'Unknown')] += 1
        
        # Resale properties
        resale_props = property_listings.get('resale_properties', [])
        total_resale += len(resale_props)
        
        for prop in resale_props:
            unit_types[prop.get('unit_type', 'Unknown')] += 1
            posted_by[prop.get('posted_by', 'Unknown')] += 1
    
    print(f"Total Rental Properties: {total_rental}")
    print(f"Total Resale Properties: {total_resale}")
    print(f"Total Properties: {total_rental + total_resale}")
    
    print(f"\nTop Unit Types:")
    for unit_type, count in unit_types.most_common(10):
        print(f"  {unit_type}: {count} properties")
    
    print(f"\nPosted By:")
    for poster, count in posted_by.most_common():
        print(f"  {poster}: {count} properties")

def analyze_extraction_performance(records):
    """Analyze extraction performance metrics."""
    print("\n⚡ EXTRACTION PERFORMANCE")
    print("=" * 50)
    
    total_found = 0
    total_extracted = 0
    total_failed = 0
    total_time = 0
    success_rates = []
    
    for record in records:
        summary = record.get('extraction_summary', {})
        metrics = record.get('performance_metrics', {})
        
        found = summary.get('total_rental_found', 0) + summary.get('total_resale_found', 0)
        extracted = summary.get('successful_rental_extractions', 0) + summary.get('successful_resale_extractions', 0)
        failed = summary.get('failed_extractions', 0)
        
        total_found += found
        total_extracted += extracted
        total_failed += failed
        total_time += metrics.get('total_extraction_time', 0)
        
        if found > 0:
            success_rates.append((extracted / found) * 100)
    
    print(f"Total Properties Found: {total_found}")
    print(f"Total Properties Extracted: {total_extracted}")
    print(f"Total Failed Extractions: {total_failed}")
    print(f"Overall Success Rate: {(total_extracted / total_found * 100):.1f}%" if total_found > 0 else "N/A")
    print(f"Total Extraction Time: {total_time:.1f} seconds")
    print(f"Average Time per Project: {(total_time / len(records)):.1f} seconds" if records else "N/A")
    
    if success_rates:
        avg_success_rate = sum(success_rates) / len(success_rates)
        print(f"Average Project Success Rate: {avg_success_rate:.1f}%")

def show_sample_properties(records, limit=3):
    """Show sample properties from the data."""
    print(f"\n🏠 SAMPLE PROPERTIES (showing {limit})")
    print("=" * 50)
    
    count = 0
    for record in records:
        if count >= limit:
            break
            
        project_name = record.get('raw_data', {}).get('basicDetails', {}).get('projectName', 'Unknown Project')
        property_listings = record.get('property_listings', {})
        
        print(f"\nProject: {project_name}")
        print(f"URL: {record.get('url', 'N/A')}")
        
        # Show rental properties
        rental_props = property_listings.get('rental_properties', [])
        if rental_props:
            print(f"  Rental Properties ({len(rental_props)}):")
            for prop in rental_props[:2]:  # Show first 2
                print(f"    - {prop.get('unit_type', 'N/A')} | {prop.get('size', 'N/A')} | {prop.get('price', 'N/A')}")
        
        # Show resale properties
        resale_props = property_listings.get('resale_properties', [])
        if resale_props:
            print(f"  Resale Properties ({len(resale_props)}):")
            for prop in resale_props[:2]:  # Show first 2
                print(f"    - {prop.get('unit_type', 'N/A')} | {prop.get('size', 'N/A')} | {prop.get('price', 'N/A')}")
        
        count += 1

def main():
    """Main analysis function."""
    # Default file paths
    files_to_check = [
        "sample_raw_bangalore_99acres.ndjson",
        "raw_bangalore_99acres.ndjson"
    ]
    
    data_file = None
    for file_path in files_to_check:
        if os.path.exists(file_path):
            data_file = file_path
            break
    
    if not data_file:
        print("❌ No data files found. Looking for:")
        for file_path in files_to_check:
            print(f"  - {file_path}")
        return
    
    print(f"📁 Analyzing data from: {data_file}")
    print("=" * 60)
    
    # Load data
    records = load_ndjson(data_file)
    
    if not records:
        print("❌ No records found in the data file.")
        return
    
    # Run analysis
    analyze_projects(records)
    analyze_properties(records)
    analyze_extraction_performance(records)
    show_sample_properties(records)
    
    print(f"\n✅ Analysis complete! Processed {len(records)} project records.")

if __name__ == "__main__":
    main()