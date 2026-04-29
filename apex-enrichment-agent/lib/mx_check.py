"""DNS MX record validation for email domains.

A domain without MX records cannot receive email. We flag (not reject)
emails on such domains for human review — DNS misconfigs happen.
"""
from __future__ import annotations

import logging
from functools import lru_cache

import dns.resolver
import dns.exception

logger = logging.getLogger(__name__)

_resolver = dns.resolver.Resolver()
_resolver.timeout = 5
_resolver.lifetime = 5


@lru_cache(maxsize=2048)
def has_mx(domain: str) -> bool:
    """Return True if the domain has at least one MX record."""
    try:
        answers = _resolver.resolve(domain, "MX")
        return len(answers) > 0
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
        return False
    except dns.exception.DNSException as e:
        logger.warning(f"DNS error for {domain}: {e}")
        return False


def email_mx_valid(email: str) -> bool:
    if "@" not in email:
        return False
    return has_mx(email.rsplit("@", 1)[1].lower())
