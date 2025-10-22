"""
Captcha detection and handling component.

This module implements captcha detection logic using URL pattern matching
and content-based analysis to identify captcha challenges.
"""

import re
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import requests

from ..core.base_interfaces import BaseHandler
from ..models.data_models import CaptchaEvent, CaptchaType


class CaptchaDetectionResult:
    """
    Structured response for captcha detection with metadata.
    """
    
    def __init__(
        self,
        is_captcha: bool,
        captcha_type: CaptchaType = CaptchaType.UNKNOWN,
        detection_method: str = "unknown",
        confidence: float = 0.0,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.is_captcha = is_captcha
        self.captcha_type = captcha_type
        self.detection_method = detection_method
        self.confidence = confidence
        self.metadata = metadata or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert detection result to dictionary."""
        return {
            "is_captcha": self.is_captcha,
            "captcha_type": self.captcha_type.value,
            "detection_method": self.detection_method,
            "confidence": self.confidence,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat()
        }


class CaptchaHandler(BaseHandler):
    """
    Component responsible for detecting and handling captcha challenges.
    
    Implements URL pattern matching and content-based detection to identify
    captcha challenges with structured response and metadata.
    """
    
    def __init__(self, config=None):
        super().__init__(config)
        self.captcha_url_patterns = []
        self.content_patterns = []
        self.captcha_events = []
        self._setup_default_patterns()
    
    def initialize(self) -> bool:
        """Initialize the captcha handler with patterns and configuration."""
        try:
            self._setup_default_patterns()
            self._load_custom_patterns()
            self.logger.info("CaptchaHandler initialized successfully")
            self._initialized = True
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize CaptchaHandler: {e}")
            return False
    
    def cleanup(self) -> None:
        """Clean up captcha handler resources."""
        self.captcha_events.clear()
        self.logger.info("CaptchaHandler cleaned up")
    
    def handle(self, data: Union[requests.Response, str]) -> CaptchaDetectionResult:
        """
        Handle captcha detection and processing.
        
        Args:
            data: HTTP response object or URL string to analyze
            
        Returns:
            CaptchaDetectionResult: Structured detection result with metadata
        """
        if isinstance(data, requests.Response):
            return self._detect_captcha_from_response(data)
        elif isinstance(data, str):
            return self._detect_captcha_from_url(data)
        else:
            return CaptchaDetectionResult(
                is_captcha=False,
                detection_method="unsupported_data_type",
                metadata={"error": f"Unsupported data type: {type(data)}"}
            )
    
    def can_handle(self, data: Any) -> bool:
        """
        Check if this handler can process the given data.
        
        Args:
            data: Input data to check
            
        Returns:
            bool: True if data is a requests.Response or string URL
        """
        return isinstance(data, (requests.Response, str))
    
    def detect_captcha(self, response: requests.Response) -> bool:
        """
        Detect captcha challenges in HTTP responses.
        
        Args:
            response: HTTP response to analyze
            
        Returns:
            bool: True if captcha is detected
        """
        result = self._detect_captcha_from_response(response)
        return result.is_captcha
    
    def get_captcha_type(self, response: requests.Response) -> str:
        """
        Get the type of captcha detected in the response.
        
        Args:
            response: HTTP response to analyze
            
        Returns:
            str: Type of captcha detected
        """
        result = self._detect_captcha_from_response(response)
        return result.captcha_type.value
    
    def should_use_browser(self, captcha_count: int) -> bool:
        """
        Determine if browser automation should be used based on captcha encounters.
        
        Args:
            captcha_count: Number of captcha encounters
            
        Returns:
            bool: True if browser automation should be used
        """
        # Use browser after 2 captcha encounters
        return captcha_count >= 2
    
    def log_captcha_event(
        self,
        url: str,
        captcha_type: str,
        detection_result: Optional[CaptchaDetectionResult] = None,
        user_agent: Optional[str] = None,
        proxy_used: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> None:
        """
        Log captcha event with timestamps and URLs for analysis.
        
        Args:
            url: URL where captcha was encountered
            captcha_type: Type of captcha detected
            detection_result: Optional detection result with metadata
            user_agent: User agent used for the request
            proxy_used: Proxy server used for the request
            session_id: Session ID if applicable
        """
        try:
            # Convert string captcha_type to enum
            if isinstance(captcha_type, str):
                captcha_type_enum = CaptchaType(captcha_type)
            else:
                captcha_type_enum = captcha_type
        except ValueError:
            captcha_type_enum = CaptchaType.UNKNOWN
        
        # Create captcha event
        event = CaptchaEvent(
            url=url,
            captcha_type=captcha_type_enum,
            detection_method=detection_result.detection_method if detection_result else "unknown",
            response_status_code=detection_result.metadata.get('status_code') if detection_result else None,
            response_content_snippet=detection_result.metadata.get('content_snippet') if detection_result else None,
            user_agent=user_agent,
            proxy_used=proxy_used,
            session_id=session_id
        )
        
        # Store event for metrics
        self.captcha_events.append(event)
        
        # Log the event
        self.logger.warning(
            f"Captcha detected - URL: {url}, Type: {captcha_type_enum.value}, "
            f"Method: {event.detection_method}, Timestamp: {event.timestamp.isoformat()}"
        )
        
        # Update metrics
        self.increment_metric('total_captcha_encounters')
        self.increment_metric(f'captcha_type_{captcha_type_enum.value}')
        
        if detection_result:
            self.increment_metric(f'detection_method_{detection_result.detection_method}')
            self.update_metric('last_captcha_confidence', detection_result.confidence)
        
        # Update URL-specific metrics
        url_key = f'captcha_count_{self._get_domain_from_url(url)}'
        self.increment_metric(url_key)
        
        self.logger.info(f"Captcha event logged and metrics updated for {url}")
    
    def get_captcha_metrics(self) -> Dict[str, Any]:
        """
        Get captcha encounter metrics and statistics.
        
        Returns:
            Dict containing captcha metrics and analysis
        """
        metrics = self.get_metrics().copy()
        
        # Calculate additional metrics from events
        total_events = len(self.captcha_events)
        metrics['total_captcha_events'] = total_events
        
        if total_events > 0:
            # Calculate metrics by type
            type_counts = {}
            detection_method_counts = {}
            url_counts = {}
            
            for event in self.captcha_events:
                # Count by type
                type_key = event.captcha_type.value
                type_counts[type_key] = type_counts.get(type_key, 0) + 1
                
                # Count by detection method
                method_key = event.detection_method
                detection_method_counts[method_key] = detection_method_counts.get(method_key, 0) + 1
                
                # Count by URL/domain
                domain = self._get_domain_from_url(event.url)
                url_counts[domain] = url_counts.get(domain, 0) + 1
            
            metrics['captcha_types'] = type_counts
            metrics['detection_methods'] = detection_method_counts
            metrics['domains_with_captcha'] = url_counts
            
            # Calculate rates
            recent_events = [e for e in self.captcha_events if (datetime.now() - e.timestamp).total_seconds() < 3600]
            metrics['captcha_rate_last_hour'] = len(recent_events)
            
            # Most common captcha type
            if type_counts:
                most_common_type = max(type_counts.items(), key=lambda x: x[1])
                metrics['most_common_captcha_type'] = most_common_type[0]
                metrics['most_common_captcha_count'] = most_common_type[1]
        
        return metrics
    
    def get_captcha_events(self, limit: Optional[int] = None) -> List[CaptchaEvent]:
        """
        Get recent captcha events for analysis.
        
        Args:
            limit: Maximum number of events to return (most recent first)
            
        Returns:
            List of CaptchaEvent objects
        """
        # Sort by timestamp (most recent first)
        sorted_events = sorted(self.captcha_events, key=lambda x: x.timestamp, reverse=True)
        
        if limit:
            return sorted_events[:limit]
        return sorted_events
    
    def clear_old_events(self, max_age_hours: int = 24) -> int:
        """
        Clear captcha events older than specified hours.
        
        Args:
            max_age_hours: Maximum age of events to keep in hours
            
        Returns:
            Number of events removed
        """
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        initial_count = len(self.captcha_events)
        
        self.captcha_events = [
            event for event in self.captcha_events
            if event.timestamp > cutoff_time
        ]
        
        removed_count = initial_count - len(self.captcha_events)
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} old captcha events")
        
        return removed_count
    
    def _setup_default_patterns(self) -> None:
        """Set up default captcha detection patterns."""
        # URL patterns for captcha detection
        self.captcha_url_patterns = [
            re.compile(r'verifycaptcha', re.IGNORECASE),
            re.compile(r'captcha', re.IGNORECASE),
            re.compile(r'verify.*human', re.IGNORECASE),
            re.compile(r'robot.*check', re.IGNORECASE),
            re.compile(r'security.*check', re.IGNORECASE),
            re.compile(r'challenge', re.IGNORECASE),
            re.compile(r'recaptcha', re.IGNORECASE),
            re.compile(r'hcaptcha', re.IGNORECASE),
        ]
        
        # Content patterns for captcha detection
        self.content_patterns = [
            {
                'pattern': re.compile(r'captcha', re.IGNORECASE),
                'type': CaptchaType.CONTENT_BASED,
                'confidence': 0.8
            },
            {
                'pattern': re.compile(r'verify.*human', re.IGNORECASE),
                'type': CaptchaType.CONTENT_BASED,
                'confidence': 0.9
            },
            {
                'pattern': re.compile(r'recaptcha', re.IGNORECASE),
                'type': CaptchaType.RECAPTCHA,
                'confidence': 0.95
            },
            {
                'pattern': re.compile(r'hcaptcha', re.IGNORECASE),
                'type': CaptchaType.VISUAL,
                'confidence': 0.95
            },
            {
                'pattern': re.compile(r'robot.*check', re.IGNORECASE),
                'type': CaptchaType.CONTENT_BASED,
                'confidence': 0.7
            },
            {
                'pattern': re.compile(r'security.*verification', re.IGNORECASE),
                'type': CaptchaType.CONTENT_BASED,
                'confidence': 0.8
            },
            {
                'pattern': re.compile(r'prove.*human', re.IGNORECASE),
                'type': CaptchaType.CONTENT_BASED,
                'confidence': 0.85
            },
        ]
    
    def _load_custom_patterns(self) -> None:
        """Load custom patterns from configuration if available."""
        if not self.config:
            return
        
        # Load custom URL patterns
        custom_url_patterns = self.config.get('captcha_url_patterns', [])
        for pattern in custom_url_patterns:
            try:
                self.captcha_url_patterns.append(re.compile(pattern, re.IGNORECASE))
            except re.error as e:
                self.logger.warning(f"Invalid URL pattern '{pattern}': {e}")
        
        # Load custom content patterns
        custom_content_patterns = self.config.get('captcha_content_patterns', [])
        for pattern_config in custom_content_patterns:
            try:
                pattern = re.compile(pattern_config['pattern'], re.IGNORECASE)
                captcha_type = CaptchaType(pattern_config.get('type', 'content_based'))
                confidence = pattern_config.get('confidence', 0.5)
                
                self.content_patterns.append({
                    'pattern': pattern,
                    'type': captcha_type,
                    'confidence': confidence
                })
            except (re.error, ValueError, KeyError) as e:
                self.logger.warning(f"Invalid content pattern config: {e}")
    
    def _detect_captcha_from_response(self, response: requests.Response) -> CaptchaDetectionResult:
        """
        Detect captcha from HTTP response using URL and content analysis.
        
        Args:
            response: HTTP response to analyze
            
        Returns:
            CaptchaDetectionResult: Detection result with metadata
        """
        # First check URL patterns
        url_result = self._detect_captcha_from_url(response.url)
        if url_result.is_captcha:
            # Add response metadata
            url_result.metadata.update({
                'status_code': response.status_code,
                'content_length': len(response.content),
                'headers': dict(response.headers)
            })
            # Log the captcha event
            self.log_captcha_event(response.url, url_result.captcha_type.value, url_result)
            return url_result
        
        # Then check content patterns
        content_result = self._detect_captcha_from_content(response.text)
        if content_result.is_captcha:
            # Add response metadata
            content_result.metadata.update({
                'url': response.url,
                'status_code': response.status_code,
                'content_length': len(response.content),
                'headers': dict(response.headers)
            })
            # Log the captcha event
            self.log_captcha_event(response.url, content_result.captcha_type.value, content_result)
            return content_result
        
        # No captcha detected
        return CaptchaDetectionResult(
            is_captcha=False,
            detection_method="url_and_content_analysis",
            metadata={
                'url': response.url,
                'status_code': response.status_code,
                'content_length': len(response.content)
            }
        )
    
    def _detect_captcha_from_url(self, url: str) -> CaptchaDetectionResult:
        """
        Detect captcha from URL using pattern matching.
        
        Args:
            url: URL to analyze
            
        Returns:
            CaptchaDetectionResult: Detection result
        """
        for pattern in self.captcha_url_patterns:
            if pattern.search(url):
                result = CaptchaDetectionResult(
                    is_captcha=True,
                    captcha_type=CaptchaType.URL_PATTERN,
                    detection_method="url_pattern_matching",
                    confidence=0.9,
                    metadata={
                        'url': url,
                        'matched_pattern': pattern.pattern
                    }
                )
                # Log the captcha event if this is a standalone URL check
                if not hasattr(self, '_in_response_detection'):
                    self.log_captcha_event(url, result.captcha_type.value, result)
                return result
        
        return CaptchaDetectionResult(
            is_captcha=False,
            detection_method="url_pattern_matching",
            metadata={'url': url}
        )
    
    def _detect_captcha_from_content(self, content: str) -> CaptchaDetectionResult:
        """
        Detect captcha from response content using pattern matching.
        
        Args:
            content: Response content to analyze
            
        Returns:
            CaptchaDetectionResult: Detection result
        """
        best_match = None
        highest_confidence = 0.0
        
        for pattern_config in self.content_patterns:
            pattern = pattern_config['pattern']
            if pattern.search(content):
                confidence = pattern_config['confidence']
                if confidence > highest_confidence:
                    highest_confidence = confidence
                    best_match = pattern_config
        
        if best_match:
            # Get a snippet of the matched content for analysis
            match = best_match['pattern'].search(content)
            snippet_start = max(0, match.start() - 50)
            snippet_end = min(len(content), match.end() + 50)
            content_snippet = content[snippet_start:snippet_end]
            
            return CaptchaDetectionResult(
                is_captcha=True,
                captcha_type=best_match['type'],
                detection_method="content_pattern_matching",
                confidence=highest_confidence,
                metadata={
                    'matched_pattern': best_match['pattern'].pattern,
                    'content_snippet': content_snippet,
                    'match_position': match.span()
                }
            )
        
        return CaptchaDetectionResult(
            is_captcha=False,
            detection_method="content_pattern_matching",
            metadata={'content_length': len(content)}
        )
    
    def _get_domain_from_url(self, url: str) -> str:
        """
        Extract domain from URL for metrics tracking.
        
        Args:
            url: URL to extract domain from
            
        Returns:
            Domain string
        """
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc or 'unknown'
        except Exception:
            return 'unknown'