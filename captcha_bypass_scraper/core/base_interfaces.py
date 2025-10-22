"""
Base interfaces and abstract classes for all components in the scraper system.

This module defines the core interfaces that all components must implement,
ensuring consistent behavior and enabling proper dependency injection.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import logging


class BaseComponent(ABC):
    """
    Base abstract class for all scraper system components.
    
    Provides common functionality like logging, configuration access,
    and lifecycle management that all components need.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the base component.
        
        Args:
            config: Optional configuration dictionary for the component
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._initialized = False
        self._metrics = {}
        
    @abstractmethod
    def initialize(self) -> bool:
        """
        Initialize the component with its configuration.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        """Clean up resources when the component is no longer needed."""
        pass
    
    def is_initialized(self) -> bool:
        """Check if the component has been properly initialized."""
        return self._initialized
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for this component."""
        return self._metrics.copy()
    
    def update_metric(self, key: str, value: Any) -> None:
        """Update a metric value."""
        self._metrics[key] = value
    
    def increment_metric(self, key: str, amount: int = 1) -> None:
        """Increment a numeric metric."""
        self._metrics[key] = self._metrics.get(key, 0) + amount


class BaseHandler(BaseComponent):
    """
    Base abstract class for handler components.
    
    Handlers are responsible for processing specific types of events or data,
    such as captcha detection or response processing.
    """
    
    @abstractmethod
    def handle(self, data: Any) -> Any:
        """
        Handle the provided data and return processed result.
        
        Args:
            data: Input data to be processed
            
        Returns:
            Processed result
        """
        pass
    
    @abstractmethod
    def can_handle(self, data: Any) -> bool:
        """
        Check if this handler can process the given data.
        
        Args:
            data: Input data to check
            
        Returns:
            bool: True if this handler can process the data
        """
        pass


class BaseManager(BaseComponent):
    """
    Base abstract class for manager components.
    
    Managers are responsible for maintaining and coordinating resources,
    such as proxy pools or session states.
    """
    
    @abstractmethod
    def get_resource(self, **kwargs) -> Optional[Any]:
        """
        Get a resource from this manager.
        
        Returns:
            The requested resource or None if unavailable
        """
        pass
    
    @abstractmethod
    def release_resource(self, resource: Any) -> None:
        """
        Release a resource back to the manager.
        
        Args:
            resource: The resource to release
        """
        pass
    
    @abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of managed resources.
        
        Returns:
            Dictionary containing status information
        """
        pass


class ConfigurableComponent(BaseComponent):
    """
    Base class for components that need runtime configuration updates.
    """
    
    @abstractmethod
    def update_config(self, new_config: Dict[str, Any]) -> bool:
        """
        Update the component configuration at runtime.
        
        Args:
            new_config: New configuration dictionary
            
        Returns:
            bool: True if update was successful
        """
        pass
    
    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate a configuration dictionary.
        
        Args:
            config: Configuration to validate
            
        Returns:
            bool: True if configuration is valid
        """
        pass


class MonitorableComponent(BaseComponent):
    """
    Base class for components that provide detailed monitoring capabilities.
    """
    
    @abstractmethod
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get the current health status of the component.
        
        Returns:
            Dictionary containing health information
        """
        pass
    
    @abstractmethod
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for the component.
        
        Returns:
            Dictionary containing performance data
        """
        pass


class RetryableComponent(BaseComponent):
    """
    Base class for components that support retry mechanisms.
    """
    
    @abstractmethod
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """
        Determine if an operation should be retried.
        
        Args:
            error: The exception that occurred
            attempt: Current attempt number (1-based)
            
        Returns:
            bool: True if the operation should be retried
        """
        pass
    
    @abstractmethod
    def get_retry_delay(self, attempt: int) -> float:
        """
        Get the delay before the next retry attempt.
        
        Args:
            attempt: Current attempt number (1-based)
            
        Returns:
            float: Delay in seconds
        """
        pass