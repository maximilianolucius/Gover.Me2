"""Public API for the fact checking validation system."""

from .core import ClaimValidator, validate_claims

__all__ = ["ClaimValidator", "validate_claims"]
