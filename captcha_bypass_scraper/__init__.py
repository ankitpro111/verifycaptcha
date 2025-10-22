"""
Enhanced Captcha Bypass Scraper System

A modular web scraping system designed to handle captcha challenges and anti-bot measures
while scraping real estate data from 99acres.com and similar websites.
"""

__version__ = "1.0.0"
__author__ = "Captcha Bypass Scraper Team"

from .core.scraper_system import ScraperSystem
from .handlers.captcha_handler import CaptchaHandler
from .managers.proxy_manager import ProxyManager
from .managers.session_manager import SessionManager
from .emulators.browser_emulator import BrowserEmulator
from .utils.rate_limiter import RateLimiter
from .config.config_manager import ConfigManager

__all__ = [
    "ScraperSystem",
    "CaptchaHandler", 
    "ProxyManager",
    "SessionManager",
    "BrowserEmulator",
    "RateLimiter",
    "ConfigManager"
]