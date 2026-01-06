"""
Input Validation for Analytics API
==================================

Provides validation helpers for API parameters with clear error messages.

Usage
-----
    from repeater.analytics.validation import (
        validate_hours, validate_limit, validate_hash_param
    )
    
    # In API endpoint
    try:
        hours = validate_hours(hours_param)
        limit = validate_limit(limit_param)
        node_hash = validate_hash_param(node_param, "node")
    except ValidationError as e:
        return e.to_response()

Validation Functions
-------------------
    validate_positive_int(value, name, max_value=None)
    validate_hours(value, default=168)
    validate_limit(value, default=100, max_limit=1000)
    validate_hash_param(value, name)
    validate_min_certainty(value, default=5)

Error Response Format
--------------------
All validation errors use the same format as errors.py for consistency:

    {
        "success": False,
        "error": {
            "code": "INVALID_PARAMETER",
            "message": "...",
            "httpStatus": 400,
            "details": { "parameter": "...", "value": "..." }
        }
    }
"""

from dataclasses import dataclass
from typing import Any, Optional, Dict, Union
from .config import Config
from .utils import validate_hash, normalize_hash, sanitize_for_sql
# Import error utilities for consistent response format
from .errors import ErrorCode, api_error


@dataclass
class ValidationError(Exception):
    """
    Validation error with details for API response.
    
    Uses the same response format as errors.api_error() for consistency.
    """
    
    parameter: str
    message: str
    value: Any = None
    
    def to_response(self) -> Dict[str, Any]:
        """
        Convert to API error response dict.
        
        Uses api_error() internally for consistent formatting with
        other error types in the analytics module.
        """
        return api_error(
            ErrorCode.INVALID_PARAMETER,
            self.message,
            details={
                "parameter": self.parameter,
                "value": str(self.value) if self.value is not None else None,
            }
        )
    
    def __str__(self) -> str:
        return f"Invalid '{self.parameter}': {self.message}"


def validate_positive_int(
    value: Any,
    name: str,
    default: Optional[int] = None,
    min_value: int = 1,
    max_value: Optional[int] = None,
) -> int:
    """
    Validate and convert value to positive integer.
    
    Args:
        value: Input value (may be string from query param)
        name: Parameter name for error messages
        default: Default value if None or empty
        min_value: Minimum allowed value (default: 1)
        max_value: Maximum allowed value (optional)
        
    Returns:
        Validated integer
        
    Raises:
        ValidationError: If value is invalid
    """
    # Handle None/empty with default
    if value is None or value == "":
        if default is not None:
            return default
        raise ValidationError(name, f"'{name}' is required", value)
    
    # Convert to int
    try:
        int_val = int(value)
    except (ValueError, TypeError):
        raise ValidationError(name, f"must be an integer, got '{value}'", value)
    
    # Check bounds
    if int_val < min_value:
        raise ValidationError(
            name,
            f"must be at least {min_value}, got {int_val}",
            value
        )
    
    if max_value is not None and int_val > max_value:
        raise ValidationError(
            name,
            f"must be at most {max_value}, got {int_val}",
            value
        )
    
    return int_val


def validate_positive_float(
    value: Any,
    name: str,
    default: Optional[float] = None,
    min_value: float = 0.0,
    max_value: Optional[float] = None,
) -> float:
    """
    Validate and convert value to positive float.
    
    Args:
        value: Input value
        name: Parameter name for error messages
        default: Default value if None or empty
        min_value: Minimum allowed value (default: 0.0)
        max_value: Maximum allowed value (optional)
        
    Returns:
        Validated float
        
    Raises:
        ValidationError: If value is invalid
    """
    if value is None or value == "":
        if default is not None:
            return default
        raise ValidationError(name, f"'{name}' is required", value)
    
    try:
        float_val = float(value)
    except (ValueError, TypeError):
        raise ValidationError(name, f"must be a number, got '{value}'", value)
    
    if float_val < min_value:
        raise ValidationError(
            name,
            f"must be at least {min_value}, got {float_val}",
            value
        )
    
    if max_value is not None and float_val > max_value:
        raise ValidationError(
            name,
            f"must be at most {max_value}, got {float_val}",
            value
        )
    
    return float_val


def validate_hours(
    value: Any,
    default: Optional[int] = None,
) -> int:
    """
    Validate hours parameter.
    
    Uses API config for defaults and limits.
    
    Args:
        value: Hours value from query param
        default: Override default (uses Config.API.DEFAULT_HOURS if None)
        
    Returns:
        Validated hours integer
        
    Raises:
        ValidationError: If hours is invalid
    """
    _default = default if default is not None else Config.API.DEFAULT_HOURS
    
    return validate_positive_int(
        value,
        "hours",
        default=_default,
        min_value=1,
        max_value=Config.API.MAX_HOURS,
    )


def validate_limit(
    value: Any,
    default: Optional[int] = None,
    max_limit: Optional[int] = None,
) -> int:
    """
    Validate limit parameter.
    
    Args:
        value: Limit value from query param
        default: Override default (uses Config.API.DEFAULT_LIMIT if None)
        max_limit: Override max (uses Config.API.MAX_LIMIT if None)
        
    Returns:
        Validated limit integer
        
    Raises:
        ValidationError: If limit is invalid
    """
    _default = default if default is not None else Config.API.DEFAULT_LIMIT
    _max = max_limit if max_limit is not None else Config.API.MAX_LIMIT
    
    return validate_positive_int(
        value,
        "limit",
        default=_default,
        min_value=1,
        max_value=_max,
    )


def validate_min_certainty(
    value: Any,
    default: Optional[int] = None,
) -> int:
    """
    Validate min_certainty parameter.
    
    Args:
        value: Certainty value from query param
        default: Override default (uses Config.GRAPH.DEFAULT_MIN_CERTAIN_COUNT if None)
        
    Returns:
        Validated certainty integer
        
    Raises:
        ValidationError: If certainty is invalid
    """
    _default = default if default is not None else Config.GRAPH.DEFAULT_MIN_CERTAIN_COUNT
    
    return validate_positive_int(
        value,
        "min_certainty",
        default=_default,
        min_value=0,
        max_value=10000,  # Reasonable upper bound
    )


def validate_hash_param(
    value: Any,
    name: str = "hash",
    required: bool = True,
    sanitize: bool = True,
) -> Optional[str]:
    """
    Validate, sanitize, and normalize a hash parameter.
    
    This function provides defense-in-depth for hash parameters:
    1. Validates format (hex characters only)
    2. Sanitizes by stripping non-hex characters (if sanitize=True)
    3. Normalizes to 0xUPPERCASE format
    
    Args:
        value: Hash value from query param
        name: Parameter name for error messages
        required: Whether the parameter is required
        sanitize: Whether to sanitize by removing non-hex chars (default True)
        
    Returns:
        Normalized hash string (0xUPPERCASE format), or None if not required and empty
        
    Raises:
        ValidationError: If hash is invalid or empty after sanitization
        
    Security:
        Even though parameterized queries prevent SQL injection, sanitization
        provides defense-in-depth against edge cases and ensures hash values
        only contain expected characters.
    """
    if value is None or value == "":
        if required:
            raise ValidationError(name, f"'{name}' is required", value)
        return None
    
    str_value = str(value)
    
    # Optionally sanitize first (removes non-hex characters)
    if sanitize:
        sanitized = sanitize_for_sql(str_value)
        if not sanitized:
            raise ValidationError(
                name,
                f"contains no valid hex characters, got '{value}'",
                value
            )
        # If sanitization changed the value significantly, it was malformed
        # Allow minor differences (case, 0x prefix)
        original_hex = str_value.upper().replace("0X", "")
        sanitized_hex = sanitized.upper().replace("0X", "")
        if original_hex != sanitized_hex:
            raise ValidationError(
                name,
                f"contains invalid characters, got '{value}'",
                value
            )
        str_value = sanitized
    
    # Check if it's a valid hash format
    if not validate_hash(str_value):
        raise ValidationError(
            name,
            f"must be a valid hex hash (e.g., '0xABCD1234' or 'ABCD1234'), got '{value}'",
            value
        )
    
    return normalize_hash(str_value)


def validate_bool(
    value: Any,
    name: str,
    default: bool = False,
) -> bool:
    """
    Validate boolean parameter.
    
    Accepts: true/false, 1/0, yes/no (case insensitive)
    
    Args:
        value: Boolean value from query param
        name: Parameter name for error messages
        default: Default value if None or empty
        
    Returns:
        Validated boolean
        
    Raises:
        ValidationError: If value is invalid
    """
    if value is None or value == "":
        return default
    
    str_val = str(value).lower().strip()
    
    if str_val in ("true", "1", "yes"):
        return True
    if str_val in ("false", "0", "no"):
        return False
    
    raise ValidationError(
        name,
        f"must be a boolean (true/false), got '{value}'",
        value
    )


def validate_string_choice(
    value: Any,
    name: str,
    choices: list,
    default: Optional[str] = None,
    case_sensitive: bool = False,
) -> str:
    """
    Validate string is one of allowed choices.
    
    Args:
        value: String value from query param
        name: Parameter name for error messages
        choices: List of allowed values
        default: Default value if None or empty
        case_sensitive: Whether comparison is case-sensitive
        
    Returns:
        Validated string
        
    Raises:
        ValidationError: If value is not in choices
    """
    if value is None or value == "":
        if default is not None:
            return default
        raise ValidationError(name, f"'{name}' is required", value)
    
    str_val = str(value)
    
    if case_sensitive:
        if str_val in choices:
            return str_val
    else:
        lower_val = str_val.lower()
        for choice in choices:
            if choice.lower() == lower_val:
                return choice
    
    choices_str = ", ".join(f"'{c}'" for c in choices)
    raise ValidationError(
        name,
        f"must be one of [{choices_str}], got '{value}'",
        value
    )
