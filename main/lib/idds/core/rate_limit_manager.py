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
Rate limit manager for per-user and per-VO rate limiting.
"""

from threading import RLock
from idds.core.rate_limiter import RateLimiter, RateLimitConfig, RateLimitError


class RateLimitManager:
    """
    Manages rate limiters for different users and VOs.
    
    Maintains separate rate limiters per user/VO and provides
    centralized limit enforcement.
    """
    
    # Default rate limit configurations
    DEFAULT_GLOBAL_CONFIG = RateLimitConfig(
        requests_per_minute=1000,
        requests_per_hour=100000,
        max_burst=200,
        per_user=False,
        per_vo=False
    )
    
    DEFAULT_USER_CONFIG = RateLimitConfig(
        requests_per_minute=100,
        requests_per_hour=10000,
        max_burst=50,
        per_user=True,
        per_vo=False
    )
    
    DEFAULT_VO_CONFIG = RateLimitConfig(
        requests_per_minute=500,
        requests_per_hour=50000,
        max_burst=100,
        per_user=False,
        per_vo=True
    )
    
    def __init__(self, global_config=None, user_config=None, vo_config=None):
        """
        Initialize rate limit manager.
        
        :param global_config: Global rate limit configuration
        :param user_config: Per-user rate limit configuration
        :param vo_config: Per-VO rate limit configuration
        """
        self.global_config = global_config or self.DEFAULT_GLOBAL_CONFIG
        self.user_config = user_config or self.DEFAULT_USER_CONFIG
        self.vo_config = vo_config or self.DEFAULT_VO_CONFIG
        
        self.global_limiter = RateLimiter(self.global_config)
        self.user_limiters = {}  # user_id -> RateLimiter
        self.vo_limiters = {}    # vo_id -> RateLimiter
        
        self._lock = RLock()
    
    def _get_or_create_user_limiter(self, user_id):
        """Get or create rate limiter for a user."""
        if user_id not in self.user_limiters:
            self.user_limiters[user_id] = RateLimiter(self.user_config)
        return self.user_limiters[user_id]
    
    def _get_or_create_vo_limiter(self, vo_id):
        """Get or create rate limiter for a VO."""
        if vo_id not in self.vo_limiters:
            self.vo_limiters[vo_id] = RateLimiter(self.vo_config)
        return self.vo_limiters[vo_id]
    
    def check_request(self, tokens=1, user_id=None, vo_id=None):
        """
        Check if a request is within rate limits.
        
        Applies all applicable rate limits (global, per-user, per-VO).
        
        :param tokens: Number of tokens needed
        :param user_id: User identifier (for per-user limiting)
        :param vo_id: VO identifier (for per-VO limiting)
        :return: (allowed: bool, headers: dict)
        
        :raises RateLimitError: If rate limit exceeded
        """
        with self._lock:
            headers = {}
            
            # Check global limit
            allowed, reason, retry_after = self.global_limiter.check_limit(tokens)
            if not allowed:
                headers.update(self._create_rate_limit_headers(
                    self.global_limiter.get_stats(), retry_after
                ))
                raise RateLimitError(f"Global {reason}", retry_after)
            
            # Check per-user limit
            if user_id and self.user_config.per_user:
                user_limiter = self._get_or_create_user_limiter(user_id)
                allowed, reason, retry_after = user_limiter.check_limit(tokens)
                if not allowed:
                    headers.update(self._create_rate_limit_headers(
                        user_limiter.get_stats(), retry_after
                    ))
                    raise RateLimitError(f"User {reason}", retry_after)
            
            # Check per-VO limit
            if vo_id and self.vo_config.per_vo:
                vo_limiter = self._get_or_create_vo_limiter(vo_id)
                allowed, reason, retry_after = vo_limiter.check_limit(tokens)
                if not allowed:
                    headers.update(self._create_rate_limit_headers(
                        vo_limiter.get_stats(), retry_after
                    ))
                    raise RateLimitError(f"VO {reason}", retry_after)
            
            # All checks passed - actually consume tokens
            # Note: RateLimiter.check_limit already consumes tokens from
            # its internal token bucket and sliding window counter.
            # Do not consume again here to avoid double-counting.
            
            # Return headers for response
            return self._get_rate_limit_headers(
                self.global_limiter, user_id, vo_id
            )
    
    def _create_rate_limit_headers(self, stats, retry_after):
        """Create rate limit headers for error response."""
        headers = {}
        if stats:
            headers['X-RateLimit-Limit'] = str(stats.get('minute_capacity', 'N/A'))
            headers['X-RateLimit-Remaining'] = str(stats.get('minute_available_tokens', 'N/A'))
        if retry_after:
            headers['Retry-After'] = str(int(retry_after))
        return headers
    
    def _get_rate_limit_headers(self, limiter, user_id=None, vo_id=None):
        """Get rate limit headers for success response."""
        stats = limiter.get_stats()
        headers = {
            'X-RateLimit-Limit': str(stats['minute_capacity']),
            'X-RateLimit-Remaining': str(int(stats['minute_available_tokens'])),
            'X-RateLimit-Reset': str(int(stats['minute_capacity'] / limiter.config.requests_per_minute * 60))
        }
        return headers
    
    def reset_user_limits(self, user_id):
        """Reset rate limits for a specific user."""
        with self._lock:
            if user_id in self.user_limiters:
                self.user_limiters[user_id] = RateLimiter(self.user_config)
    
    def reset_vo_limits(self, vo_id):
        """Reset rate limits for a specific VO."""
        with self._lock:
            if vo_id in self.vo_limiters:
                self.vo_limiters[vo_id] = RateLimiter(self.vo_config)
    
    def reset_global_limits(self):
        """Reset global rate limits."""
        with self._lock:
            self.global_limiter = RateLimiter(self.global_config)
    
    def get_stats(self, user_id=None, vo_id=None):
        """Get statistics for rate limiters."""
        with self._lock:
            stats = {
                'global': self.global_limiter.get_stats()
            }
            if user_id and user_id in self.user_limiters:
                stats[f'user_{user_id}'] = self.user_limiters[user_id].get_stats()
            if vo_id and vo_id in self.vo_limiters:
                stats[f'vo_{vo_id}'] = self.vo_limiters[vo_id].get_stats()
            return stats
    
    def update_config(self, config_type='global', **kwargs):
        """
        Update rate limit configuration.
        
        :param config_type: 'global', 'user', or 'vo'
        :param kwargs: Configuration parameters to update
        """
        with self._lock:
            if config_type == 'global':
                config = self.global_config
                self.global_limiter = RateLimiter(config)
            elif config_type == 'user':
                config = self.user_config
                self.user_limiters.clear()
            elif config_type == 'vo':
                config = self.vo_config
                self.vo_limiters.clear()
            else:
                raise ValueError(f"Unknown config type: {config_type}")
            
            # Update config parameters
            for key, value in kwargs.items():
                if hasattr(config, key):
                    setattr(config, key, value)


# Global rate limit manager instance
_manager_instance = None


def get_rate_limit_manager():
    """Get the global rate limit manager instance."""
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = RateLimitManager()
    return _manager_instance


def set_rate_limit_manager(manager):
    """Set the global rate limit manager instance."""
    global _manager_instance
    _manager_instance = manager
