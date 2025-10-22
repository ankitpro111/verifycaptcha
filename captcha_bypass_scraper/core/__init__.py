"""Core components for the scraper system."""

from .scraper_system import ScraperSystem
from .base_interfaces import BaseComponent, BaseHandler, BaseManager

__all__ = ["ScraperSystem", "BaseComponent", "BaseHandler", "BaseManager"]