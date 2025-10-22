#!/usr/bin/env python3
"""
Example usage of the enhanced captcha bypass scraper system.

This script demonstrates how to use the scraper system with the new
modular architecture and configuration management.
"""

import logging
from pathlib import Path
from captcha_bypass_scraper import ScraperSystem

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('scraper_example.log')
        ]
    )

def main():
    """Main example function."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Initialize scraper system with configuration
    config_file = Path("config/scraper_config.json")
    scraper = ScraperSystem(config_file)
    
    try:
        # Initialize the system
        if not scraper.initialize():
            logger.error("Failed to initialize scraper system")
            return
        
        # Example URLs to scrape (replace with actual URLs)
        urls = [
            "https://www.99acres.com/example-property-1",
            "https://www.99acres.com/example-property-2",
            "https://www.99acres.com/example-property-3"
        ]
        
        logger.info(f"Starting to process {len(urls)} URLs")
        
        # Process URLs
        results = scraper.process_urls(
            urls=urls,
            output_file="output/scraping_results.ndjson"
        )
        
        # Display statistics
        stats = scraper.get_scraping_stats()
        logger.info("Scraping completed!")
        logger.info(f"Total processed: {stats['total_processed']}")
        logger.info(f"Successful: {stats['successful']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Success rate: {stats['success_rate']:.2%}")
        logger.info(f"Captcha encounters: {stats['captcha_encountered']}")
        logger.info(f"Runtime: {stats['runtime_seconds']:.1f} seconds")
        
        # Display component status
        component_status = scraper.get_component_status()
        logger.info("Component Status:")
        for component, status in component_status.items():
            logger.info(f"  {component}: {'✅' if status.get('initialized', False) else '❌'}")
        
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        
    finally:
        # Clean up resources
        scraper.cleanup()
        logger.info("Scraper system cleaned up")

if __name__ == "__main__":
    main()