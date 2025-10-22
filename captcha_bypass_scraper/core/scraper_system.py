"""
Main scraper system that orchestrates all components.

This module contains the primary ScraperSystem class that coordinates
all other components to provide a unified scraping interface.
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..core.base_interfaces import BaseComponent
from ..config.config_manager import ConfigManager
from ..models.data_models import ScrapingResult, ScrapingMethod


class ScraperSystem(BaseComponent):
    """
    Main scraper system that orchestrates all components.
    
    This class serves as the primary interface for the scraping system,
    coordinating captcha handlers, proxy managers, session managers,
    browser emulators, and rate limiters to provide robust scraping capabilities.
    """
    
    def __init__(self, config_file: Optional[Union[str, Path]] = None):
        """
        Initialize the scraper system.
        
        Args:
            config_file: Path to configuration file
        """
        super().__init__()
        
        # Initialize configuration manager
        self.config_manager = ConfigManager(config_file)
        
        # Component references (will be initialized later)
        self.captcha_handler = None
        self.proxy_manager = None
        self.session_manager = None
        self.browser_emulator = None
        self.rate_limiter = None
        
        # Threading and synchronization
        self.lock = threading.Lock()
        self.executor = None
        
        # Tracking and metrics
        self.processed_urls = set()
        self.results = []
        self.start_time = None
        
        # Configuration shortcuts
        self.max_workers = 5
        self.save_interval = 100
        self.output_file = None
        
    def initialize(self) -> bool:
        """
        Initialize the scraper system and all its components.
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Initialize configuration manager first
            if not self.config_manager.initialize():
                self.logger.error("Failed to initialize configuration manager")
                return False
            
            # Update configuration shortcuts
            self.max_workers = self.config_manager.get('scraping.max_workers', 5)
            self.save_interval = self.config_manager.get('scraping.save_progress_interval', 100)
            
            # Initialize thread pool executor
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
            
            # Initialize components (lazy loading - will be done when first needed)
            self._initialized = True
            self.start_time = datetime.now()
            
            self.logger.info("Scraper system initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize scraper system: {e}")
            return False
    
    def cleanup(self) -> None:
        """Clean up all system resources."""
        try:
            # Shutdown thread pool
            if self.executor:
                self.executor.shutdown(wait=True)
            
            # Cleanup components
            components = [
                self.captcha_handler,
                self.proxy_manager,
                self.session_manager,
                self.browser_emulator,
                self.rate_limiter
            ]
            
            for component in components:
                if component and hasattr(component, 'cleanup'):
                    try:
                        component.cleanup()
                    except Exception as e:
                        self.logger.warning(f"Error cleaning up component {component.__class__.__name__}: {e}")
            
            # Cleanup configuration manager
            if self.config_manager:
                self.config_manager.cleanup()
            
            self.logger.info("Scraper system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def process_urls(self, urls: List[str], output_file: Optional[str] = None) -> List[ScrapingResult]:
        """
        Process a list of URLs using the scraping system.
        
        Args:
            urls: List of URLs to scrape
            output_file: Optional file to save results to
            
        Returns:
            List of scraping results
        """
        if not self._initialized:
            raise RuntimeError("Scraper system not initialized. Call initialize() first.")
        
        self.output_file = output_file
        self.results = []
        
        # Filter out already processed URLs
        urls_to_process = [url for url in urls if url not in self.processed_urls]
        
        if not urls_to_process:
            self.logger.info("No new URLs to process")
            return self.results
        
        self.logger.info(f"Processing {len(urls_to_process)} URLs with {self.max_workers} workers")
        
        # Submit all URLs to thread pool
        futures = []
        for idx, url in enumerate(urls_to_process, 1):
            future = self.executor.submit(self._process_single_url, idx, len(urls_to_process), url)
            futures.append(future)
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    with self.lock:
                        self.results.append(result)
                        self.processed_urls.add(result.url)
                        
                        # Save progress periodically
                        if len(self.results) % self.save_interval == 0:
                            self._save_progress()
                            
            except Exception as e:
                self.logger.error(f"Error processing URL: {e}")
        
        # Final save
        if self.output_file:
            self._save_progress()
        
        self.logger.info(f"Completed processing {len(self.results)} URLs")
        return self.results
    
    def _process_single_url(self, idx: int, total: int, url: str) -> Optional[ScrapingResult]:
        """
        Process a single URL with error handling and retry logic.
        
        Args:
            idx: Current URL index
            total: Total number of URLs
            url: URL to process
            
        Returns:
            ScrapingResult or None if processing failed
        """
        try:
            self.logger.info(f"[{idx}/{total}] Processing: {url}")
            
            # Initialize components if needed (lazy loading)
            self._ensure_components_initialized()
            
            # Check rate limiting
            if self.rate_limiter and not self.rate_limiter.can_make_request(url):
                self.logger.info(f"[{idx}/{total}] Rate limited, skipping: {url}")
                return None
            
            # Try HTTP scraping first
            result = self._try_http_scraping(url)
            
            # If captcha encountered, try browser emulation
            if result and result.captcha_encountered and self.browser_emulator:
                self.logger.info(f"[{idx}/{total}] Captcha detected, trying browser emulation: {url}")
                browser_result = self._try_browser_scraping(url)
                if browser_result and browser_result.success:
                    result = browser_result
            
            # Update rate limiter based on result
            if self.rate_limiter:
                if result and result.success:
                    self.rate_limiter.record_success(url)
                else:
                    self.rate_limiter.record_failure(url)
            
            if result:
                status = "✅" if result.success else "❌"
                self.logger.info(f"[{idx}/{total}] {status} {result.method_used.value}: {url}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"[{idx}/{total}] Exception processing {url}: {e}")
            return ScrapingResult(
                url=url,
                success=False,
                error_message=str(e),
                method_used=ScrapingMethod.HTTP
            )
    
    def _try_http_scraping(self, url: str) -> Optional[ScrapingResult]:
        """
        Attempt to scrape URL using HTTP requests.
        
        Args:
            url: URL to scrape
            
        Returns:
            ScrapingResult or None
        """
        # This is a placeholder - actual implementation will be in task 7
        # For now, return a basic result structure
        return ScrapingResult(
            url=url,
            success=False,
            error_message="HTTP scraping not yet implemented",
            method_used=ScrapingMethod.HTTP
        )
    
    def _try_browser_scraping(self, url: str) -> Optional[ScrapingResult]:
        """
        Attempt to scrape URL using browser emulation.
        
        Args:
            url: URL to scrape
            
        Returns:
            ScrapingResult or None
        """
        # This is a placeholder - actual implementation will be in task 6
        # For now, return a basic result structure
        return ScrapingResult(
            url=url,
            success=False,
            error_message="Browser scraping not yet implemented",
            method_used=ScrapingMethod.BROWSER
        )
    
    def _ensure_components_initialized(self) -> None:
        """Ensure all required components are initialized (lazy loading)."""
        # This will be implemented as components are created in subsequent tasks
        pass
    
    def _save_progress(self) -> None:
        """Save current results to output file."""
        if not self.output_file or not self.results:
            return
        
        try:
            output_path = Path(self.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as NDJSON (newline-delimited JSON)
            with open(output_path, 'w', encoding='utf-8') as f:
                for result in self.results:
                    f.write(json.dumps(result.to_dict(), ensure_ascii=False) + '\n')
            
            self.logger.info(f"Progress saved: {len(self.results)} results to {self.output_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save progress: {e}")
    
    def get_scraping_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive scraping statistics.
        
        Returns:
            Dictionary containing scraping statistics
        """
        if not self.results:
            return {"total_processed": 0}
        
        successful = sum(1 for r in self.results if r.success)
        failed = len(self.results) - successful
        captcha_encountered = sum(1 for r in self.results if r.captcha_encountered)
        
        # Method breakdown
        method_stats = {}
        for method in ScrapingMethod:
            count = sum(1 for r in self.results if r.method_used == method)
            method_stats[method.value] = count
        
        # Calculate runtime
        runtime_seconds = 0
        if self.start_time:
            runtime_seconds = (datetime.now() - self.start_time).total_seconds()
        
        return {
            "total_processed": len(self.results),
            "successful": successful,
            "failed": failed,
            "success_rate": successful / len(self.results) if self.results else 0,
            "captcha_encountered": captcha_encountered,
            "captcha_rate": captcha_encountered / len(self.results) if self.results else 0,
            "method_breakdown": method_stats,
            "runtime_seconds": runtime_seconds,
            "urls_per_minute": (len(self.results) / (runtime_seconds / 60)) if runtime_seconds > 0 else 0,
            "start_time": self.start_time.isoformat() if self.start_time else None
        }
    
    def handle_captcha_fallback(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Handle captcha fallback scenario for a specific URL.
        
        Args:
            url: URL that encountered captcha
            
        Returns:
            Extracted data or None if fallback failed
        """
        # This will be implemented when browser emulator is ready
        self.logger.info(f"Captcha fallback requested for: {url}")
        return None
    
    def get_component_status(self) -> Dict[str, Any]:
        """
        Get status of all system components.
        
        Returns:
            Dictionary containing component status information
        """
        status = {
            "scraper_system": {
                "initialized": self._initialized,
                "start_time": self.start_time.isoformat() if self.start_time else None,
                "processed_urls": len(self.processed_urls),
                "active_workers": self.max_workers
            },
            "config_manager": {
                "initialized": self.config_manager.is_initialized() if self.config_manager else False,
                "config_file": str(self.config_manager.config_file) if self.config_manager and self.config_manager.config_file else None
            }
        }
        
        # Add component status when they're implemented
        components = [
            ("captcha_handler", self.captcha_handler),
            ("proxy_manager", self.proxy_manager),
            ("session_manager", self.session_manager),
            ("browser_emulator", self.browser_emulator),
            ("rate_limiter", self.rate_limiter)
        ]
        
        for name, component in components:
            if component:
                status[name] = {
                    "initialized": component.is_initialized() if hasattr(component, 'is_initialized') else True,
                    "metrics": component.get_metrics() if hasattr(component, 'get_metrics') else {}
                }
            else:
                status[name] = {"initialized": False, "loaded": False}
        
        return status