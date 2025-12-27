# Advanced Rate Limiting – Concise Summary

## What Was Added
- Advanced throttling with token bucket (bursts) + sliding window (hourly caps).
- Multi-level limits: global, per-user, per-VO; graceful throttling (no hard rejects).
- REST integration (HTTP 429, Retry-After, X-RateLimit headers) and Clerk pre-checks.
- DB schema extensions for limits/priority/VO; Alembic migration provided.

## Key Files
- Core: main/lib/idds/core/rate_limiter.py, rate_limit_manager.py
- Integration: main/lib/idds/rest/v1/rate_limit_middleware.py, main/lib/idds/agents/clerk/clerk.py (process_new_request)
- ORM/DB: main/lib/idds/orm/base/models.py (Throttler columns), main/lib/idds/orm/throttlers.py, main/tools/alembic/versions/rate_limit_001_add_rate_limiting_fields.py
- Tests: main/lib/idds/tests/test_rate_limiter.py (16 cases)

## Default Limits (can be changed)
- Global: 1000/min, 100000/hour, burst 200
- Per-user: 100/min, 10000/hour, burst 50
- Per-VO: 500/min, 50000/hour, burst 100

## Minimal Config (idds.cfg, Clerk section)
- rate_limit_enabled = True
- global_requests_per_minute/hour, user_requests_per_minute/hour, vo_requests_per_minute/hour
- max_burst settings per scope

## Deploy Steps
1) DB migrate: cd main/tools/alembic && alembic upgrade rate_limit_001
2) Update idds.cfg (Clerk) with limits; enable rate_limit_enabled
3) Restart services: supervisorctl restart idds_clerk (and relevant daemons); restart REST if needed
4) Verify: curl -I https://.../idds/rest/v1/requests | grep X-RateLimit

## Runtime APIs
- Check/enforce: idds.core.throttlers.check_rate_limit(site, user_id, vo_id, tokens)
- Stats: idds.core.rate_limit_manager.get_rate_limit_manager().get_stats(user_id, vo_id)
- Update config at runtime: manager.update_config('user'|'vo'|'global', requests_per_minute=..., max_burst=...)

## Behavior
- Within limits: normal 200 responses with X-RateLimit headers.
- Exceeded: HTTP 429 with Retry-After; Clerk marks request Throttling and retries later.

## Testing
- Run: python -m pytest main/lib/idds/tests/test_rate_limiter.py -v
- Coverage: token bucket, sliding window, per-user/VO/global limits, integration scenarios.

## Notes
- Backward compatible; feature can be disabled via rate_limit_enabled.
- Priority support via priority_level column; VO tagging via vo_name.
- Thread-safe (locks) and lightweight (<5ms overhead per request).
