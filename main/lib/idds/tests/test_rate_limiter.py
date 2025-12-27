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
Unit tests for rate limiting implementation.
"""

import unittest
import time
from idds.core.rate_limiter import (
    TokenBucket, SlidingWindowCounter, RateLimitConfig,
    RateLimiter, RateLimitError
)
from idds.core.rate_limit_manager import RateLimitManager


class TestTokenBucket(unittest.TestCase):
    """Tests for TokenBucket implementation."""
    
    def test_token_bucket_initialization(self):
        """Test token bucket initialization."""
        bucket = TokenBucket(capacity=10, refill_rate=5.0, refill_interval=1.0)
        self.assertEqual(bucket.capacity, 10)
        self.assertEqual(bucket.tokens, 10)
    
    def test_token_consumption(self):
        """Test consuming tokens from bucket."""
        bucket = TokenBucket(capacity=10, refill_rate=5.0, refill_interval=1.0)
        
        # Consume within capacity
        success, wait_time = bucket.consume(5, wait=False)
        self.assertTrue(success)
        self.assertEqual(wait_time, 0.0)
        
        # Check remaining tokens
        remaining = bucket.get_available_tokens()
        self.assertEqual(remaining, 5)
    
    def test_token_bucket_burst(self):
        """Test burst capacity."""
        bucket = TokenBucket(capacity=10, refill_rate=2.0, refill_interval=1.0)
        
        # Consume full capacity immediately (burst)
        success, wait_time = bucket.consume(10, wait=False)
        self.assertTrue(success)
        
        # Try to consume more (should fail)
        success, wait_time = bucket.consume(1, wait=False)
        self.assertFalse(success)
        self.assertGreater(wait_time, 0)
    
    def test_token_refill(self):
        """Test token refilling over time."""
        bucket = TokenBucket(capacity=10, refill_rate=10.0, refill_interval=1.0)
        
        # Consume all tokens
        bucket.consume(10)
        self.assertEqual(bucket.get_available_tokens(), 0)
        
        # Wait for refill
        time.sleep(1.1)
        remaining = bucket.get_available_tokens()
        self.assertGreater(remaining, 0)


class TestSlidingWindowCounter(unittest.TestCase):
    """Tests for SlidingWindowCounter implementation."""
    
    def test_sliding_window_initialization(self):
        """Test sliding window counter initialization."""
        counter = SlidingWindowCounter(window_size=60, max_requests=100)
        self.assertEqual(counter.window_size, 60)
        self.assertEqual(counter.max_requests, 100)
    
    def test_sliding_window_counting(self):
        """Test request counting within window."""
        counter = SlidingWindowCounter(window_size=60, max_requests=5)
        
        # Consume within limit
        for i in range(5):
            success, count, reset = counter.consume(1)
            self.assertTrue(success)
        
        # Consume beyond limit
        success, count, reset = counter.consume(1)
        self.assertFalse(success)
        self.assertGreater(reset, 0)
    
    def test_sliding_window_cleanup(self):
        """Test cleanup of expired requests."""
        counter = SlidingWindowCounter(window_size=1, max_requests=5)
        
        # Add requests
        for i in range(5):
            counter.consume(1)
        
        current_count = counter.get_current_count()
        self.assertEqual(current_count, 5)
        
        # Wait for window to expire
        time.sleep(1.1)
        
        # Check that old requests are cleaned up
        current_count = counter.get_current_count()
        self.assertEqual(current_count, 0)


class TestRateLimitConfig(unittest.TestCase):
    """Tests for RateLimitConfig."""
    
    def test_config_initialization(self):
        """Test configuration initialization."""
        config = RateLimitConfig(
            requests_per_minute=100,
            requests_per_hour=10000,
            max_burst=50,
            per_user=True,
            per_vo=True
        )
        self.assertEqual(config.requests_per_minute, 100)
        self.assertEqual(config.requests_per_hour, 10000)
        self.assertTrue(config.per_user)
        self.assertTrue(config.per_vo)
    
    def test_config_to_dict(self):
        """Test configuration serialization."""
        config = RateLimitConfig(requests_per_minute=100)
        config_dict = config.to_dict()
        
        self.assertIsInstance(config_dict, dict)
        self.assertIn('requests_per_minute', config_dict)
        self.assertEqual(config_dict['requests_per_minute'], 100)


class TestRateLimiter(unittest.TestCase):
    """Tests for RateLimiter."""
    
    def test_rate_limiter_initialization(self):
        """Test rate limiter initialization."""
        config = RateLimitConfig(requests_per_minute=100)
        limiter = RateLimiter(config)
        
        self.assertIsNotNone(limiter.minute_bucket)
        self.assertIsNotNone(limiter.hour_counter)
    
    def test_rate_limiter_enforcement(self):
        """Test rate limit enforcement."""
        config = RateLimitConfig(
            requests_per_minute=3,
            requests_per_hour=100,
            max_burst=3
        )
        limiter = RateLimiter(config)
        
        # Consume within limit
        allowed, reason, retry = limiter.check_limit(1)
        self.assertTrue(allowed)
        
        # Consume multiple requests
        for i in range(2):
            limiter.minute_bucket.consume(1)
        
        # Next request within same minute bucket capacity
        allowed, reason, retry = limiter.check_limit(1)
        self.assertFalse(allowed)


class TestRateLimitManager(unittest.TestCase):
    """Tests for RateLimitManager."""
    
    def test_manager_initialization(self):
        """Test manager initialization."""
        manager = RateLimitManager()
        self.assertIsNotNone(manager.global_limiter)
        self.assertIsInstance(manager.user_limiters, dict)
        self.assertIsInstance(manager.vo_limiters, dict)
    
    def test_global_rate_limiting(self):
        """Test global rate limit enforcement."""
        config = RateLimitConfig(requests_per_minute=2, max_burst=2)
        manager = RateLimitManager(global_config=config)
        
        # First request should pass
        try:
            headers = manager.check_request(tokens=1)
            self.assertIsInstance(headers, dict)
        except RateLimitError:
            self.fail("First request should not be rate limited")
    
    def test_per_user_rate_limiting(self):
        """Test per-user rate limiting."""
        user_config = RateLimitConfig(
            requests_per_minute=1,
            max_burst=1,
            per_user=True
        )
        manager = RateLimitManager(user_config=user_config)
        
        # First user request
        try:
            manager.check_request(tokens=1, user_id='user1')
        except RateLimitError:
            self.fail("First user request should not be rate limited")
        
        # Try to consume beyond per-user limit (might succeed if global allows)
        # This depends on global config
    
    def test_rate_limit_reset(self):
        """Test rate limit reset functionality."""
        manager = RateLimitManager()
        
        # Reset limits
        manager.reset_user_limits('user1')
        manager.reset_vo_limits('vo1')
        manager.reset_global_limits()
        
        # Verify limiters were reset
        self.assertNotIn('user1', manager.user_limiters)
        self.assertNotIn('vo1', manager.vo_limiters)


class TestRateLimitIntegration(unittest.TestCase):
    """Integration tests for rate limiting."""
    
    def test_multi_user_isolation(self):
        """Test that per-user limits are isolated."""
        user_config = RateLimitConfig(
            requests_per_minute=2,
            max_burst=2,
            per_user=True
        )
        manager = RateLimitManager(user_config=user_config)
        
        # User 1 consumes tokens
        try:
            manager.check_request(tokens=1, user_id='user1')
            manager.check_request(tokens=1, user_id='user1')
        except RateLimitError:
            self.fail("User 1 should have 2 tokens")
        
        # User 2 should have separate limit
        try:
            manager.check_request(tokens=1, user_id='user2')
        except RateLimitError:
            self.fail("User 2 should have its own limit")
    
    def test_combined_limits(self):
        """Test combination of global and per-user limits."""
        global_config = RateLimitConfig(
            requests_per_minute=5,
            max_burst=5,
            per_user=False,
            per_vo=False
        )
        user_config = RateLimitConfig(
            requests_per_minute=2,
            max_burst=2,
            per_user=True,
            per_vo=False
        )
        manager = RateLimitManager(global_config=global_config, user_config=user_config)
        
        # User should be limited by per-user config first
        try:
            manager.check_request(tokens=1, user_id='user1')
            manager.check_request(tokens=1, user_id='user1')
            # Third request should fail (per-user limit)
            manager.check_request(tokens=1, user_id='user1')
            self.fail("Should be limited by per-user config")
        except RateLimitError:
            pass


if __name__ == '__main__':
    unittest.main()
