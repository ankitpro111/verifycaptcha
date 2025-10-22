# Enhanced Captcha Bypass Scraper System

A modular web scraping system designed to handle captcha challenges and anti-bot measures while scraping real estate data from 99acres.com and similar websites.

## Project Structure

```
captcha_bypass_scraper/
├── __init__.py                 # Main package exports
├── core/                       # Core system components
│   ├── __init__.py
│   ├── base_interfaces.py      # Abstract base classes and interfaces
│   └── scraper_system.py       # Main orchestrator class
├── handlers/                   # Event and data handlers
│   ├── __init__.py
│   └── captcha_handler.py      # Captcha detection and handling
├── managers/                   # Resource managers
│   ├── __init__.py
│   ├── proxy_manager.py        # Proxy pool management
│   └── session_manager.py      # Session state management
├── emulators/                  # Browser emulation
│   ├── __init__.py
│   └── browser_emulator.py     # Headless browser automation
├── utils/                      # Utility components
│   ├── __init__.py
│   └── rate_limiter.py         # Rate limiting and retry logic
├── config/                     # Configuration management
│   ├── __init__.py
│   └── config_manager.py       # Configuration loading and validation
└── models/                     # Data models
    ├── __init__.py
    └── data_models.py          # Data structures and enums

config/
└── scraper_config.json         # Default configuration file

example_usage.py                 # Example usage script
requirements.txt                 # Python dependencies
README.md                       # This file
```

## Features

### Current Implementation (Task 1)

- ✅ **Modular Architecture**: Clean separation of concerns with dedicated components
- ✅ **Base Interfaces**: Abstract classes ensuring consistent component behavior
- ✅ **Configuration Management**: JSON/YAML configuration with environment variable overrides
- ✅ **Data Models**: Comprehensive data structures for all system entities
- ✅ **Main Orchestrator**: ScraperSystem class coordinating all components
- ✅ **Logging and Metrics**: Built-in logging and performance tracking

### Planned Features (Future Tasks)

- 🔄 **Captcha Detection**: URL pattern and content-based captcha detection
- 🔄 **Proxy Management**: Rotating proxy pool with health monitoring
- 🔄 **Session Management**: Browser-like sessions with cookies and headers
- 🔄 **Rate Limiting**: Intelligent retry mechanisms with exponential backoff
- 🔄 **Browser Emulation**: Headless browser automation for complex scenarios
- 🔄 **OCR Integration**: Automated captcha solving capabilities

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

The system uses a JSON configuration file located at `config/scraper_config.json`. Key configuration sections include:

- **proxies**: Proxy server settings and rotation strategy
- **user_agents**: List of user agents for rotation
- **sessions**: Session management parameters
- **rate_limiting**: Retry and backoff configuration
- **captcha_handling**: Captcha detection patterns and strategies
- **browser_emulation**: Headless browser settings
- **scraping**: General scraping parameters
- **logging**: Logging configuration

### Environment Variable Overrides

You can override configuration values using environment variables:

- `SCRAPER_PROXY_ENABLED`: Enable/disable proxy usage
- `SCRAPER_MAX_WORKERS`: Number of concurrent workers
- `SCRAPER_REQUEST_TIMEOUT`: Request timeout in seconds
- `SCRAPER_LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `SCRAPER_BROWSER_HEADLESS`: Enable/disable headless browser mode

## Usage

### Basic Usage

```python
from captcha_bypass_scraper import ScraperSystem

# Initialize with configuration
scraper = ScraperSystem("config/scraper_config.json")

# Initialize the system
if scraper.initialize():
    # Process URLs
    urls = ["https://example.com/page1", "https://example.com/page2"]
    results = scraper.process_urls(urls, output_file="results.ndjson")
    
    # Get statistics
    stats = scraper.get_scraping_stats()
    print(f"Success rate: {stats['success_rate']:.2%}")
    
    # Clean up
    scraper.cleanup()
```

### Running the Example

```bash
python example_usage.py
```

## Component Architecture

### Base Interfaces

All components inherit from base classes that provide:

- **BaseComponent**: Common functionality (logging, metrics, lifecycle)
- **BaseHandler**: Event and data processing interface
- **BaseManager**: Resource management interface
- **ConfigurableComponent**: Runtime configuration updates
- **MonitorableComponent**: Health and performance monitoring
- **RetryableComponent**: Retry mechanism support

### Data Models

The system uses strongly-typed data models:

- **ScrapingResult**: Complete scraping operation result
- **ProxyInfo**: Proxy server information and metrics
- **SessionState**: Session state and tracking
- **CaptchaEvent**: Captcha encounter logging
- **RateLimitState**: Rate limiting state per URL/domain

## Development Status

This is the initial implementation (Task 1) focusing on project structure and core interfaces. Individual components will be implemented in subsequent tasks:

- Task 2: Captcha Handler implementation
- Task 3: Proxy Manager implementation  
- Task 4: Session Manager implementation
- Task 5: Rate Limiter implementation
- Task 6: Browser Emulator implementation
- Task 7: Integration and enhancement of main scraper
- Task 8: Configuration and monitoring enhancements

## Requirements

The system addresses the following requirements:

- **1.1-1.5**: Captcha detection and logging capabilities
- **2.1-2.5**: Proxy rotation and health monitoring
- **3.1-3.5**: Browser automation for complex scenarios
- **4.1-4.5**: Intelligent retry mechanisms
- **5.1-5.5**: Session persistence and realistic browsing

## License

This project is part of a real estate data collection system and is intended for educational and research purposes.