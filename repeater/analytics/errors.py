"""
Error Handling for Analytics API
================================

Standardized error codes and response formatting for consistent API responses.

Error Codes
-----------
    INVALID_PARAMETER (400): Bad input parameter
    NOT_FOUND (404): Resource not found  
    DEPENDENCY_MISSING (424): Required dependency not available
    INTERNAL_ERROR (500): Unexpected internal error
    DATABASE_ERROR (500): Database operation failed

Usage
-----
    from repeater.analytics.errors import (
        ErrorCode, AnalyticsError, api_error, api_success
    )
    
    # Raise typed exceptions
    if not validate_hash(hash_str):
        raise AnalyticsError(
            ErrorCode.INVALID_PARAMETER,
            f"Invalid hash format: {hash_str}"
        )
    
    # Build consistent responses
    return api_success(data)
    return api_error(ErrorCode.NOT_FOUND, "Node not found")
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class ErrorCode(Enum):
    """Standard error codes for API responses."""
    
    # Client errors (4xx)
    INVALID_PARAMETER = ("INVALID_PARAMETER", 400, "Invalid or malformed parameter")
    MISSING_PARAMETER = ("MISSING_PARAMETER", 400, "Required parameter missing")
    NOT_FOUND = ("NOT_FOUND", 404, "Requested resource not found")
    
    # Server errors (5xx)
    INTERNAL_ERROR = ("INTERNAL_ERROR", 500, "An internal error occurred")
    DATABASE_ERROR = ("DATABASE_ERROR", 500, "Database operation failed")
    DEPENDENCY_MISSING = ("DEPENDENCY_MISSING", 424, "Required dependency not available")
    
    def __init__(self, code: str, http_status: int, default_message: str):
        self.code = code
        self.http_status = http_status
        self.default_message = default_message


@dataclass
class AnalyticsError(Exception):
    """Exception with error code for API responses."""
    
    error_code: ErrorCode
    message: str
    details: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        return f"[{self.error_code.code}] {self.message}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to API response dict."""
        result = {
            "success": False,
            "error": {
                "code": self.error_code.code,
                "message": self.message,
                "httpStatus": self.error_code.http_status,
            }
        }
        if self.details:
            result["error"]["details"] = self.details
        return result


def api_success(data: Any, **kwargs) -> Dict[str, Any]:
    """
    Build a successful API response.
    
    Args:
        data: The response data
        **kwargs: Additional top-level fields to include
        
    Returns:
        Dict with success=True and data
        
    Example:
        >>> api_success({"nodes": [...], "edges": [...]})
        {"success": True, "data": {"nodes": [...], "edges": [...]}}
        
        >>> api_success(items, count=len(items))
        {"success": True, "data": items, "count": 5}
    """
    result = {"success": True, "data": data}
    result.update(kwargs)
    return result


def api_error(
    error_code: ErrorCode,
    message: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build an error API response.
    
    Args:
        error_code: The ErrorCode enum value
        message: Custom error message (uses default if not provided)
        details: Additional error details
        
    Returns:
        Dict with success=False and error info
        
    Example:
        >>> api_error(ErrorCode.INVALID_PARAMETER, "hours must be positive")
        {
            "success": False,
            "error": {
                "code": "INVALID_PARAMETER",
                "message": "hours must be positive",
                "httpStatus": 400
            }
        }
    """
    msg = message or error_code.default_message
    
    result = {
        "success": False,
        "error": {
            "code": error_code.code,
            "message": msg,
            "httpStatus": error_code.http_status,
        }
    }
    
    if details:
        result["error"]["details"] = details
    
    return result


def api_error_from_exception(
    exc: Exception,
    default_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
) -> Dict[str, Any]:
    """
    Build error response from an exception.
    
    If the exception is an AnalyticsError, uses its error code.
    Otherwise, uses the default code with the exception message.
    
    Args:
        exc: The exception that was raised
        default_code: Error code to use for non-AnalyticsError exceptions
        
    Returns:
        Dict with success=False and error info
    """
    if isinstance(exc, AnalyticsError):
        return exc.to_dict()
    
    return api_error(default_code, str(exc))


# Common validation error helpers

def invalid_param(param_name: str, reason: str) -> Dict[str, Any]:
    """Shorthand for invalid parameter errors."""
    return api_error(
        ErrorCode.INVALID_PARAMETER,
        f"Invalid '{param_name}': {reason}",
        details={"parameter": param_name}
    )


def missing_param(param_name: str) -> Dict[str, Any]:
    """Shorthand for missing parameter errors."""
    return api_error(
        ErrorCode.MISSING_PARAMETER,
        f"Required parameter '{param_name}' is missing",
        details={"parameter": param_name}
    )


def not_found(resource: str, identifier: str) -> Dict[str, Any]:
    """Shorthand for not found errors."""
    return api_error(
        ErrorCode.NOT_FOUND,
        f"{resource} '{identifier}' not found",
        details={"resource": resource, "identifier": identifier}
    )
