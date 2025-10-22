"""
Configuration management system for the captcha bypass scraper.

This module provides centralized configuration management with support for
JSON/YAML files, environment variables, and runtime updates.
"""

import json
import yaml
import os
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import logging
from ..core.base_interfaces import ConfigurableComponent


class ConfigManager(ConfigurableComponent):
    """
    Centralized configuration manager for the scraper system.
    
    Supports loading configuration from multiple sources and provides
    runtime configuration updates with validation.
    """
    
    DEFAULT_CONFIG = {
        "proxies": {
            "enabled": True,
            "pool_size": 10,
            "rotation_strategy": "round_robin",  # round_robin, random, performance_based
            "health_check_interval": 1800,  # 30 minutes
            "max_failures": 3,
            "timeout": 30,
            "list": []
        },
        "user_agents": {
            "rotation_enabled": True,
            "list": [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59",
                "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (Android 11; Mobile; rv:89.0) Gecko/89.0 Firefox/89.0"
            ]
        },
        "sessions": {
            "max_requests_per_session": 100,
            "session_timeout_minutes": 60,
            "cookie_persistence": True,
            "homepage_visit_required": True,
            "realistic_headers": True
        },
        "rate_limiting": {
            "enabled": True,
            "base_delay": 30,  # seconds
            "max_delay": 600,  # 10 minutes
            "backoff_factor": 2.0,
            "max_retries": 5,
            "reset_after_successes": 3,
            "human_like_delays": {
                "min": 5,
                "max": 15
            }
        },
        "captcha_handling": {
            "detection_patterns": [
                "verifycaptcha",
                "captcha",
                "recaptcha",
                "hcaptcha"
            ],
            "content_indicators": [
                "Please verify you are human",
                "Complete the captcha",
                "Security check"
            ],
            "auto_solve_enabled": False,
            "ocr_enabled": False,
            "browser_fallback": True
        },
        "browser_emulation": {
            "enabled": True,
            "headless": True,
            "browser_type": "chrome",  # chrome, firefox
            "window_size": [1920, 1080],
            "page_load_timeout": 30,
            "element_wait_timeout": 10,
            "max_sessions": 5,
            "session_reuse": True
        },
        "scraping": {
            "max_workers": 5,
            "request_timeout": 30,
            "retry_on_timeout": True,
            "save_progress_interval": 100,
            "output_format": "ndjson",  # json, ndjson, csv
            "include_metadata": True
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "file_enabled": True,
            "console_enabled": True,
            "log_file": "scraper.log"
        }
    }
    
    def __init__(self, config_file: Optional[Union[str, Path]] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Path to configuration file (JSON or YAML)
        """
        super().__init__()
        self.config_file = Path(config_file) if config_file else None
        self.config = self.DEFAULT_CONFIG.copy()
        self.logger = logging.getLogger(__name__)
        
    def initialize(self) -> bool:
        """Initialize the configuration manager."""
        try:
            if self.config_file and self.config_file.exists():
                self.load_from_file(self.config_file)
            
            # Load environment variable overrides
            self._load_env_overrides()
            
            # Validate the final configuration
            if not self.validate_config(self.config):
                self.logger.error("Configuration validation failed")
                return False
            
            self._initialized = True
            self.logger.info("Configuration manager initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize configuration manager: {e}")
            return False
    
    def cleanup(self) -> None:
        """Clean up configuration manager resources."""
        self.logger.info("Configuration manager cleaned up")
    
    def load_from_file(self, file_path: Union[str, Path]) -> bool:
        """
        Load configuration from a file.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            bool: True if loaded successfully
        """
        file_path = Path(file_path)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.suffix.lower() in ['.yml', '.yaml']:
                    file_config = yaml.safe_load(f)
                else:
                    file_config = json.load(f)
            
            # Deep merge with default configuration
            self.config = self._deep_merge(self.DEFAULT_CONFIG, file_config)
            self.config_file = file_path
            
            self.logger.info(f"Configuration loaded from {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration from {file_path}: {e}")
            return False
    
    def save_to_file(self, file_path: Optional[Union[str, Path]] = None) -> bool:
        """
        Save current configuration to a file.
        
        Args:
            file_path: Path to save configuration (uses current config_file if None)
            
        Returns:
            bool: True if saved successfully
        """
        if file_path is None:
            file_path = self.config_file
        
        if file_path is None:
            self.logger.error("No file path specified for saving configuration")
            return False
        
        file_path = Path(file_path)
        
        try:
            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                if file_path.suffix.lower() in ['.yml', '.yaml']:
                    yaml.dump(self.config, f, default_flow_style=False, indent=2)
                else:
                    json.dump(self.config, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Configuration saved to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save configuration to {file_path}: {e}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'proxies.enabled')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any) -> bool:
        """
        Set a configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'proxies.enabled')
            value: Value to set
            
        Returns:
            bool: True if set successfully
        """
        keys = key.split('.')
        config = self.config
        
        try:
            # Navigate to the parent of the target key
            for k in keys[:-1]:
                if k not in config:
                    config[k] = {}
                config = config[k]
            
            # Set the final value
            config[keys[-1]] = value
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set configuration key {key}: {e}")
            return False
    
    def update_config(self, new_config: Dict[str, Any]) -> bool:
        """
        Update configuration with new values.
        
        Args:
            new_config: Dictionary of new configuration values
            
        Returns:
            bool: True if update was successful
        """
        try:
            # Validate new configuration
            merged_config = self._deep_merge(self.config, new_config)
            if not self.validate_config(merged_config):
                self.logger.error("New configuration failed validation")
                return False
            
            self.config = merged_config
            self.logger.info("Configuration updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update configuration: {e}")
            return False
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate a configuration dictionary.
        
        Args:
            config: Configuration to validate
            
        Returns:
            bool: True if configuration is valid
        """
        try:
            # Check required sections
            required_sections = ['proxies', 'user_agents', 'sessions', 'rate_limiting']
            for section in required_sections:
                if section not in config:
                    self.logger.error(f"Missing required configuration section: {section}")
                    return False
            
            # Validate proxy configuration
            if config['proxies']['enabled']:
                if not isinstance(config['proxies']['list'], list):
                    self.logger.error("Proxy list must be a list")
                    return False
            
            # Validate user agents
            if not isinstance(config['user_agents']['list'], list) or not config['user_agents']['list']:
                self.logger.error("User agent list must be a non-empty list")
                return False
            
            # Validate rate limiting values
            rate_config = config['rate_limiting']
            if rate_config['base_delay'] <= 0 or rate_config['max_delay'] <= 0:
                self.logger.error("Rate limiting delays must be positive")
                return False
            
            if rate_config['backoff_factor'] <= 1:
                self.logger.error("Backoff factor must be greater than 1")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation error: {e}")
            return False
    
    def get_proxy_list(self) -> List[Dict[str, Any]]:
        """Get the list of configured proxies."""
        return self.get('proxies.list', [])
    
    def get_user_agents(self) -> List[str]:
        """Get the list of configured user agents."""
        return self.get('user_agents.list', [])
    
    def is_proxy_enabled(self) -> bool:
        """Check if proxy usage is enabled."""
        return self.get('proxies.enabled', False)
    
    def is_browser_emulation_enabled(self) -> bool:
        """Check if browser emulation is enabled."""
        return self.get('browser_emulation.enabled', False)
    
    def get_captcha_patterns(self) -> List[str]:
        """Get captcha detection patterns."""
        return self.get('captcha_handling.detection_patterns', [])
    
    def _load_env_overrides(self) -> None:
        """Load configuration overrides from environment variables."""
        env_mappings = {
            'SCRAPER_PROXY_ENABLED': 'proxies.enabled',
            'SCRAPER_MAX_WORKERS': 'scraping.max_workers',
            'SCRAPER_REQUEST_TIMEOUT': 'scraping.request_timeout',
            'SCRAPER_LOG_LEVEL': 'logging.level',
            'SCRAPER_BROWSER_HEADLESS': 'browser_emulation.headless'
        }
        
        for env_var, config_key in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Convert string values to appropriate types
                if env_value.lower() in ['true', 'false']:
                    env_value = env_value.lower() == 'true'
                elif env_value.isdigit():
                    env_value = int(env_value)
                elif env_value.replace('.', '').isdigit():
                    env_value = float(env_value)
                
                self.set(config_key, env_value)
                self.logger.info(f"Configuration override from {env_var}: {config_key} = {env_value}")
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.
        
        Args:
            base: Base dictionary
            override: Dictionary to merge into base
            
        Returns:
            Merged dictionary
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result