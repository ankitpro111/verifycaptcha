"""
Data models for the captcha bypass scraper system.

This module defines all the data structures used throughout the system,
including scraping results, proxy information, session states, and events.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from enum import Enum


class ScrapingMethod(Enum):
    """Enumeration of available scraping methods."""
    HTTP = "http"
    BROWSER = "browser" 
    PROXY = "proxy"


class CaptchaType(Enum):
    """Enumeration of captcha types that can be detected."""
    URL_PATTERN = "url_pattern"
    CONTENT_BASED = "content_based"
    VISUAL = "visual"
    RECAPTCHA = "recaptcha"
    UNKNOWN = "unknown"


class ProxyProtocol(Enum):
    """Enumeration of supported proxy protocols."""
    HTTP = "http"
    HTTPS = "https"
    SOCKS5 = "socks5"


@dataclass
class ScrapingResult:
    """
    Represents the result of a scraping operation.
    
    Contains all relevant information about a scraping attempt,
    including success status, extracted data, and metadata.
    """
    url: str
    success: bool
    data: Optional[Dict[str, Any]] = None
    method_used: ScrapingMethod = ScrapingMethod.HTTP
    captcha_encountered: bool = False
    retry_count: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    error_message: Optional[str] = None
    response_time: Optional[float] = None
    status_code: Optional[int] = None
    proxy_used: Optional[str] = None
    user_agent_used: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the result to a dictionary for serialization."""
        return {
            "url": self.url,
            "success": self.success,
            "data": self.data,
            "method_used": self.method_used.value,
            "captcha_encountered": self.captcha_encountered,
            "retry_count": self.retry_count,
            "timestamp": self.timestamp.isoformat(),
            "error_message": self.error_message,
            "response_time": self.response_time,
            "status_code": self.status_code,
            "proxy_used": self.proxy_used,
            "user_agent_used": self.user_agent_used
        }


@dataclass
class ProxyInfo:
    """
    Information about a proxy server.
    
    Contains all details needed to use and monitor a proxy,
    including connection details and health metrics.
    """
    host: str
    port: int
    protocol: ProxyProtocol = ProxyProtocol.HTTP
    username: Optional[str] = None
    password: Optional[str] = None
    is_active: bool = True
    failure_count: int = 0
    success_count: int = 0
    last_used: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    average_response_time: Optional[float] = None
    
    @property
    def proxy_url(self) -> str:
        """Generate the proxy URL for use with requests."""
        if self.username and self.password:
            return f"{self.protocol.value}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.protocol.value}://{self.host}:{self.port}"
    
    @property
    def success_rate(self) -> float:
        """Calculate the success rate of this proxy."""
        total_attempts = self.success_count + self.failure_count
        if total_attempts == 0:
            return 0.0
        return self.success_count / total_attempts
    
    def mark_success(self, response_time: Optional[float] = None) -> None:
        """Mark a successful request through this proxy."""
        self.success_count += 1
        self.last_success = datetime.now()
        self.last_used = datetime.now()
        
        if response_time is not None:
            if self.average_response_time is None:
                self.average_response_time = response_time
            else:
                # Simple moving average
                self.average_response_time = (self.average_response_time + response_time) / 2
    
    def mark_failure(self) -> None:
        """Mark a failed request through this proxy."""
        self.failure_count += 1
        self.last_failure = datetime.now()
        self.last_used = datetime.now()
        
        # Deactivate proxy after 3 consecutive failures
        if self.failure_count >= 3:
            self.is_active = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert proxy info to dictionary for serialization."""
        return {
            "host": self.host,
            "port": self.port,
            "protocol": self.protocol.value,
            "username": self.username,
            "is_active": self.is_active,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "success_rate": self.success_rate,
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "last_failure": self.last_failure.isoformat() if self.last_failure else None,
            "average_response_time": self.average_response_time
        }


@dataclass
class SessionState:
    """
    State information for a scraping session.
    
    Maintains session-specific data like cookies, headers,
    and request tracking for realistic browsing simulation.
    """
    session_id: str
    cookies: Dict[str, str] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    proxy: Optional[ProxyInfo] = None
    user_agent: Optional[str] = None
    request_count: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    homepage_visited: bool = False
    max_requests: int = 100
    
    @property
    def is_expired(self) -> bool:
        """Check if the session has exceeded its maximum request limit."""
        return self.request_count >= self.max_requests
    
    @property
    def age_minutes(self) -> float:
        """Get the age of the session in minutes."""
        return (datetime.now() - self.created_at).total_seconds() / 60
    
    def increment_requests(self) -> None:
        """Increment the request count and update last activity."""
        self.request_count += 1
        self.last_activity = datetime.now()
    
    def update_cookies(self, new_cookies: Dict[str, str]) -> None:
        """Update session cookies with new values."""
        self.cookies.update(new_cookies)
        self.last_activity = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert session state to dictionary for serialization."""
        return {
            "session_id": self.session_id,
            "cookies": self.cookies,
            "headers": self.headers,
            "proxy": self.proxy.to_dict() if self.proxy else None,
            "user_agent": self.user_agent,
            "request_count": self.request_count,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "homepage_visited": self.homepage_visited,
            "max_requests": self.max_requests,
            "is_expired": self.is_expired,
            "age_minutes": self.age_minutes
        }


@dataclass
class CaptchaEvent:
    """
    Information about a captcha encounter.
    
    Records details about when and where captchas are encountered
    for analysis and strategy optimization.
    """
    url: str
    captcha_type: CaptchaType
    timestamp: datetime = field(default_factory=datetime.now)
    detection_method: str = "unknown"
    response_status_code: Optional[int] = None
    response_content_snippet: Optional[str] = None
    user_agent: Optional[str] = None
    proxy_used: Optional[str] = None
    session_id: Optional[str] = None
    solved: bool = False
    solve_method: Optional[str] = None
    solve_time_seconds: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert captcha event to dictionary for serialization."""
        return {
            "url": self.url,
            "captcha_type": self.captcha_type.value,
            "timestamp": self.timestamp.isoformat(),
            "detection_method": self.detection_method,
            "response_status_code": self.response_status_code,
            "response_content_snippet": self.response_content_snippet,
            "user_agent": self.user_agent,
            "proxy_used": self.proxy_used,
            "session_id": self.session_id,
            "solved": self.solved,
            "solve_method": self.solve_method,
            "solve_time_seconds": self.solve_time_seconds
        }


@dataclass
class RateLimitState:
    """
    State information for rate limiting per URL or domain.
    
    Tracks retry attempts, backoff delays, and success patterns
    for intelligent rate limiting decisions.
    """
    url_or_domain: str
    current_delay: float = 30.0  # Start with 30 seconds
    retry_count: int = 0
    max_retries: int = 5
    consecutive_successes: int = 0
    last_request_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    backoff_factor: float = 2.0
    max_delay: float = 600.0  # 10 minutes maximum
    
    @property
    def is_exhausted(self) -> bool:
        """Check if maximum retries have been reached."""
        return self.retry_count >= self.max_retries
    
    @property
    def next_allowed_time(self) -> Optional[datetime]:
        """Calculate when the next request is allowed."""
        if self.last_request_time is None:
            return None
        
        from datetime import timedelta
        return self.last_request_time + timedelta(seconds=self.current_delay)
    
    def can_make_request(self) -> bool:
        """Check if a request can be made now."""
        if self.is_exhausted:
            return False
        
        next_allowed = self.next_allowed_time
        if next_allowed is None:
            return True
        
        return datetime.now() >= next_allowed
    
    def record_request(self) -> None:
        """Record that a request was made."""
        self.last_request_time = datetime.now()
    
    def record_success(self) -> None:
        """Record a successful request and potentially reset backoff."""
        self.consecutive_successes += 1
        self.last_success_time = datetime.now()
        
        # Reset backoff after 3 consecutive successes
        if self.consecutive_successes >= 3:
            self.current_delay = 30.0
            self.retry_count = 0
            self.consecutive_successes = 0
    
    def record_failure(self) -> None:
        """Record a failed request and increase backoff."""
        self.retry_count += 1
        self.consecutive_successes = 0
        
        # Increase delay with exponential backoff
        self.current_delay = min(
            self.current_delay * self.backoff_factor,
            self.max_delay
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert rate limit state to dictionary for serialization."""
        return {
            "url_or_domain": self.url_or_domain,
            "current_delay": self.current_delay,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "consecutive_successes": self.consecutive_successes,
            "last_request_time": self.last_request_time.isoformat() if self.last_request_time else None,
            "last_success_time": self.last_success_time.isoformat() if self.last_success_time else None,
            "backoff_factor": self.backoff_factor,
            "max_delay": self.max_delay,
            "is_exhausted": self.is_exhausted,
            "can_make_request": self.can_make_request()
        }