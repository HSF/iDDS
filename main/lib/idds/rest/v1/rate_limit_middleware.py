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
REST API middleware for rate limiting.
"""

from flask import request, jsonify
from functools import wraps
from idds.core.throttlers import check_rate_limit
from idds.common.constants import HTTP_STATUS_CODE


class RateLimitMiddleware:
    """Middleware for enforcing rate limits on REST API endpoints."""
    
    def __init__(self, app, enabled=True):
        """
        Initialize rate limit middleware.
        
        :param app: Flask application instance
        :param enabled: Whether rate limiting is enabled
        """
        self.app = app
        self.enabled = enabled
        
        if enabled:
            app.before_request(self.check_rate_limit_before_request)
    
    def check_rate_limit_before_request(self):
        """Check rate limit before processing request."""
        if not self.enabled:
            return None
        
        # Skip rate limit check for health checks
        if request.path in ['/health', '/ping', '/health/check']:
            return None
        
        # Extract user/VO information from request
        user_id = self._get_user_id()
        vo_id = self._get_vo_id()
        site = self._get_site()
        
        # Check rate limit
        allowed, headers, error = check_rate_limit(
            site=site, user_id=user_id, vo_id=vo_id, tokens=1
        )
        
        if not allowed:
            response = jsonify({
                'status': 'error',
                'message': error,
                'code': 'RATE_LIMIT_EXCEEDED'
            })
            response.status_code = 429
            
            # Add rate limit headers
            if 'Retry-After' in headers:
                response.headers['Retry-After'] = headers['Retry-After']
            if 'X-RateLimit-Remaining' in headers:
                response.headers['X-RateLimit-Remaining'] = headers['X-RateLimit-Remaining']
            if 'X-RateLimit-Limit' in headers:
                response.headers['X-RateLimit-Limit'] = headers['X-RateLimit-Limit']
            
            return response
        
        return None
    
    def add_rate_limit_headers(self, response):
        """Add rate limit headers to response."""
        if not self.enabled:
            return response
        
        user_id = self._get_user_id()
        vo_id = self._get_vo_id()
        
        # Would add headers from rate limit stats here
        # This is called in after_request handlers
        return response
    
    def _get_user_id(self):
        """Extract user ID from request."""
        # Try different sources for user identification
        if hasattr(request, 'environ') and 'username' in request.environ:
            return request.environ.get('username')
        
        if 'X-User-ID' in request.headers:
            return request.headers.get('X-User-ID')
        
        if 'Authorization' in request.headers:
            # Could extract from token if needed
            pass
        
        return None
    
    def _get_vo_id(self):
        """Extract VO ID from request."""
        if 'X-VO-ID' in request.headers:
            return request.headers.get('X-VO-ID')
        
        if hasattr(request, 'environ') and 'vo_id' in request.environ:
            return request.environ.get('vo_id')
        
        return None
    
    def _get_site(self):
        """Extract site from request."""
        if 'X-Site' in request.headers:
            return request.headers.get('X-Site')
        
        return None


def rate_limit(tokens=1, user_id=None, vo_id=None, site=None):
    """
    Decorator for rate limiting specific endpoints.
    
    :param tokens: Number of tokens to consume
    :param user_id: Optional user ID override
    :param vo_id: Optional VO ID override
    :param site: Optional site override
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get identifiers
            uid = user_id or _get_user_id_from_request()
            vid = vo_id or _get_vo_id_from_request()
            s = site or _get_site_from_request()
            
            # Check rate limit
            allowed, headers, error = check_rate_limit(
                site=s, user_id=uid, vo_id=vid, tokens=tokens
            )
            
            if not allowed:
                response = jsonify({
                    'status': 'error',
                    'message': error,
                    'code': 'RATE_LIMIT_EXCEEDED'
                })
                response.status_code = 429
                
                if 'Retry-After' in headers:
                    response.headers['Retry-After'] = headers['Retry-After']
                
                return response
            
            # Call original function
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator


def _get_user_id_from_request():
    """Extract user ID from Flask request."""
    if hasattr(request, 'environ') and 'username' in request.environ:
        return request.environ.get('username')
    return request.headers.get('X-User-ID')


def _get_vo_id_from_request():
    """Extract VO ID from Flask request."""
    if hasattr(request, 'environ') and 'vo_id' in request.environ:
        return request.environ.get('vo_id')
    return request.headers.get('X-VO-ID')


def _get_site_from_request():
    """Extract site from Flask request."""
    return request.headers.get('X-Site')
