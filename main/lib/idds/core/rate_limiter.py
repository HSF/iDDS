#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - HSF iDDS Contributors, 2025

"""
Rate limiting implementation with token bucket algorithm.
"""

import time
from threading import Lock
from idds.common import exceptions


class TokenBucket:
    """
    Token Bucket rate limiter implementation.
    
    This algorithm allows for bursty traffic while maintaining a long-term rate limit.
    """
    
    def __init__(self, capacity, refill_rate, refill_interval=1.0):
        """
        Initialize token bucket.
        
        :param capacity: Maximum number of tokens (burst capacity)
        :param refill_rate: Number of tokens to add per refill interval
        :param refill_interval: Time in seconds between refills
        """
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate
        self.refill_interval = refill_interval
        self.last_refill = time.time()
        self._lock = Lock()
    
    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        
        # Calculate how many refill intervals have passed
        refills = int(elapsed / self.refill_interval)
        if refills > 0:
            self.tokens = min(
                self.capacity,
                self.tokens + (refills * self.refill_rate)
            )
            self.last_refill = now + (refills * self.refill_interval)
    
    def consume(self, tokens=1, wait=False):
        """
        Consume tokens from the bucket.
        
        :param tokens: Number of tokens to consume
        :param wait: If True, wait until tokens are available. If False, return immediately.
        :return: (success: bool, wait_time: float or None)
        """
        with self._lock:
            self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return (True, 0.0)
            
            if not wait:
                # Calculate how long to wait
                deficit = tokens - self.tokens
                wait_time = (deficit / self.refill_rate) * self.refill_interval
                return (False, wait_time)
            else:
                # Wait until tokens available
                deficit = tokens - self.tokens
                wait_time = (deficit / self.refill_rate) * self.refill_interval
                time.sleep(wait_time)
                self.tokens -= tokens
                return (True, wait_time)
    
    def get_available_tokens(self):
        """Get current available tokens."""
        with self._lock:
            self._refill()
            return self.tokens


class SlidingWindowCounter:
    """
    Sliding Window Counter rate limiter implementation.
    
    This tracks requests in a rolling time window.
    """
    
    def __init__(self, window_size, max_requests):
        """
        Initialize sliding window counter.
        
        :param window_size: Time window in seconds
        :param max_requests: Maximum requests allowed in the window
        """
        self.window_size = window_size
        self.max_requests = max_requests
        self.requests = []  # List of (timestamp, tokens) tuples
        self._lock = Lock()
    
    def _cleanup_old_requests(self):
        """Remove requests outside the sliding window."""
        now = time.time()
        cutoff = now - self.window_size
        self.requests = [(ts, tokens) for ts, tokens in self.requests if ts > cutoff]
    
    def consume(self, tokens=1):
        """
        Try to consume tokens within the sliding window.
        
        :param tokens: Number of tokens to consume
        :return: (success: bool, requests_in_window: int, reset_after: float)
        """
        with self._lock:
            now = time.time()
            self._cleanup_old_requests()
            
            total_requests = sum(t for _, t in self.requests)
            
            if total_requests + tokens <= self.max_requests:
                self.requests.append((now, tokens))
                return (True, total_requests + tokens, None)
            else:
                # Calculate when oldest request expires
                if self.requests:
                    oldest_time = self.requests[0][0]
                    reset_after = oldest_time + self.window_size - now
                else:
                    reset_after = self.window_size
                
                return (False, total_requests, reset_after)
    
    def get_current_count(self):
        """Get current request count in the window."""
        with self._lock:
            self._cleanup_old_requests()
            return sum(t for _, t in self.requests)


class RateLimitConfig:
    """Configuration for rate limits."""
    
    def __init__(self, requests_per_minute=100, requests_per_hour=10000,
                 max_burst=50, per_user=False, per_vo=False):
        """
        Initialize rate limit configuration.
        
        :param requests_per_minute: Requests allowed per minute
        :param requests_per_hour: Requests allowed per hour
        :param max_burst: Maximum burst capacity
        :param per_user: If True, limits apply per user
        :param per_vo: If True, limits apply per VO
        """
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.max_burst = max_burst or max(requests_per_minute // 2, 10)
        self.per_user = per_user
        self.per_vo = per_vo
    
    def to_dict(self):
        """Convert configuration to dictionary."""
        return {
            'requests_per_minute': self.requests_per_minute,
            'requests_per_hour': self.requests_per_hour,
            'max_burst': self.max_burst,
            'per_user': self.per_user,
            'per_vo': self.per_vo
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create configuration from dictionary."""
        return cls(**data)


class RateLimiter:
    """
    Main rate limiter managing multiple limiting strategies.
    """
    
    def __init__(self, config):
        """
        Initialize rate limiter.
        
        :param config: RateLimitConfig instance
        """
        self.config = config
        # Token bucket for burst handling
        self.minute_bucket = TokenBucket(
            capacity=config.max_burst,
            refill_rate=config.requests_per_minute / 60.0,
            refill_interval=1.0
        )
        # Sliding window for long-term limits
        self.hour_counter = SlidingWindowCounter(
            window_size=3600,
            max_requests=config.requests_per_hour
        )
        self._lock = Lock()
    
    def check_limit(self, tokens=1):
        """
        Check if request is within rate limits.
        
        :param tokens: Number of tokens needed
        :return: (allowed: bool, reason: str, retry_after: float or None)
        """
        with self._lock:
            # Check minute limit first (stricter)
            minute_ok, wait_time = self.minute_bucket.consume(tokens, wait=False)
            if not minute_ok:
                return (False, f"Rate limit exceeded. Retry after {wait_time:.1f}s", wait_time)
            
            # Check hour limit
            hour_ok, count, reset_after = self.hour_counter.consume(tokens)
            if not hour_ok:
                return (False, f"Hourly quota exceeded. Reset after {reset_after:.1f}s", reset_after)
            
            # Both checks passed - actually consume the tokens
            return (True, None, None)
    
    def get_stats(self):
        """Get current rate limiter statistics."""
        return {
            'minute_available_tokens': self.minute_bucket.get_available_tokens(),
            'minute_capacity': self.minute_bucket.capacity,
            'hour_current_count': self.hour_counter.get_current_count(),
            'hour_limit': self.hour_counter.max_requests
        }


class RateLimitError(exceptions.IDDSException):
    """Exception raised when rate limit is exceeded."""
    
    def __init__(self, message, retry_after=None):
        """
        Initialize rate limit error.
        
        :param message: Error message
        :param retry_after: Time in seconds to retry after
        """
        super().__init__(message)
        self.retry_after = retry_after
