"""
Hash Utilities for Analytics Module
====================================

Centralized functions for hash normalization, validation, and manipulation.
All hash-related operations should use these utilities to ensure consistency.

Supported Hash Formats
----------------------
    Full hash:      "0xABCD1234" or "ABCD1234" (normalized to "0xABCD1234")
    Prefix:         "AB" (2-character hex prefix)
    Raw hex:        "abcd1234" (any case, normalized to uppercase)

Validation Rules
----------------
    - Full hashes must be valid hexadecimal
    - Prefixes are always uppercase 2-character hex
    - Empty/None inputs return empty string or None (no exceptions)

Usage
-----
    >>> from repeater.analytics.utils import normalize_hash, get_prefix, validate_hash
    >>> 
    >>> # Normalize to standard format
    >>> normalize_hash("abcd1234")
    '0xABCD1234'
    >>> normalize_hash("0xABCD1234")
    '0xABCD1234'
    >>> 
    >>> # Extract prefix
    >>> get_prefix("0xABCD1234")
    'AB'
    >>> get_prefix("AB")
    'AB'
    >>> 
    >>> # Validate hash format
    >>> validate_hash("0xABCD1234")
    True
    >>> validate_hash("not-a-hash")
    False
"""

import re
from typing import Optional, Tuple

# Regex patterns for hash validation
HEX_PATTERN = re.compile(r'^[0-9A-Fa-f]+$')
FULL_HASH_PATTERN = re.compile(r'^(0x)?[0-9A-Fa-f]{2,}$')
PREFIX_PATTERN = re.compile(r'^[0-9A-Fa-f]{2}$')


def normalize_hash(node_input: Optional[str]) -> str:
    """
    Normalize node hash to standard format: 0xUPPERCASE.
    
    Args:
        node_input: Hash in any format (e.g., "abcd1234", "0xABCD1234", "AB")
        
    Returns:
        Normalized hash with "0x" prefix and uppercase hex digits.
        Returns empty string for None/empty input.
        
    Examples:
        >>> normalize_hash("abcd1234")
        '0xABCD1234'
        >>> normalize_hash("0xabcd1234")
        '0xABCD1234'
        >>> normalize_hash("AB")
        '0xAB'
        >>> normalize_hash(None)
        ''
    """
    if not node_input:
        return ""
    
    node = str(node_input).strip().upper()
    
    # Remove existing 0x prefix for uniform handling
    if node.startswith("0X"):
        node = node[2:]
    
    return f"0x{node}" if node else ""


def get_prefix(full_hash: Optional[str]) -> str:
    """
    Extract 2-character prefix from full hash.
    
    Handles all common formats consistently:
        - "0xABCDEF12" -> "AB"
        - "ABCDEF12" -> "AB"  
        - "AB" -> "AB"
        - "ab" -> "AB"
    
    Args:
        full_hash: Full hash or prefix string
        
    Returns:
        Uppercase 2-character prefix, or empty string if invalid
        
    Examples:
        >>> get_prefix("0xABCD1234")
        'AB'
        >>> get_prefix("abcd1234")
        'AB'
        >>> get_prefix("AB")
        'AB'
        >>> get_prefix("")
        ''
    """
    if not full_hash:
        return ""
    
    h = str(full_hash).strip().upper()
    
    # Remove 0x prefix
    if h.startswith("0X"):
        h = h[2:]
    
    return h[:2] if len(h) >= 2 else h


def validate_hash(hash_str: Optional[str]) -> bool:
    """
    Validate that a string is a valid hash format.
    
    Valid formats:
        - "0xABCD1234" (with prefix, any length >= 2 hex chars)
        - "ABCD1234" (no prefix, any length >= 2 hex chars)
        
    Args:
        hash_str: String to validate
        
    Returns:
        True if valid hash format, False otherwise
        
    Examples:
        >>> validate_hash("0xABCD1234")
        True
        >>> validate_hash("ABCD1234")
        True
        >>> validate_hash("not-valid")
        False
        >>> validate_hash("")
        False
    """
    if not hash_str:
        return False
    
    h = str(hash_str).strip()
    return bool(FULL_HASH_PATTERN.match(h))


def validate_prefix(prefix: Optional[str]) -> bool:
    """
    Validate that a string is a valid 2-character hex prefix.
    
    Args:
        prefix: String to validate
        
    Returns:
        True if valid 2-character hex prefix, False otherwise
        
    Examples:
        >>> validate_prefix("AB")
        True
        >>> validate_prefix("ab")
        True
        >>> validate_prefix("0xAB")
        False  # Full hash, not prefix
        >>> validate_prefix("A")
        False  # Too short
    """
    if not prefix:
        return False
    
    p = str(prefix).strip()
    return bool(PREFIX_PATTERN.match(p))


def make_edge_key(hash_a: str, hash_b: str) -> str:
    """
    Create canonical edge key from two hashes.
    
    Edge keys are always sorted alphabetically to ensure consistent
    lookup regardless of traversal direction.
    
    Args:
        hash_a: First node hash (any format)
        hash_b: Second node hash (any format)
        
    Returns:
        Canonical edge key in format "HASH_A:HASH_B" (alphabetically sorted)
        
    Examples:
        >>> make_edge_key("0xABCD", "0xEF01")
        '0xABCD:0xEF01'
        >>> make_edge_key("0xEF01", "0xABCD")
        '0xABCD:0xEF01'
        >>> make_edge_key("ef01", "abcd")
        '0xABCD:0xEF01'
    """
    normalized_a = normalize_hash(hash_a)
    normalized_b = normalize_hash(hash_b)
    
    if normalized_a < normalized_b:
        return f"{normalized_a}:{normalized_b}"
    return f"{normalized_b}:{normalized_a}"


def parse_edge_key(edge_key: str) -> Tuple[str, str]:
    """
    Parse edge key back into two hashes.
    
    Args:
        edge_key: Canonical edge key (e.g., "0xABCD:0xEF01")
        
    Returns:
        Tuple of (hash_a, hash_b) in the order stored in the key
        
    Raises:
        ValueError: If edge_key is not valid format
        
    Examples:
        >>> parse_edge_key("0xABCD:0xEF01")
        ('0xABCD', '0xEF01')
    """
    if ':' not in edge_key:
        raise ValueError(f"Invalid edge key format: {edge_key}")
    
    parts = edge_key.split(':', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid edge key format: {edge_key}")
    
    return parts[0], parts[1]


def hashes_match(hash_a: Optional[str], hash_b: Optional[str]) -> bool:
    """
    Check if two hashes refer to the same node.
    
    Supports three comparison modes:
        1. Exact match: Both are full hashes and identical
        2. Prefix-to-full: One is a 2-char prefix, other is full hash with matching prefix
        3. Prefix-to-prefix: Both are 2-char prefixes and identical
    
    IMPORTANT - Prefix Matching Behavior:
        When either input is a 2-character prefix (e.g., "AB"), this function
        compares ONLY the prefixes. This means:
        
            hashes_match("0xABCD1234", "AB") -> True  (prefix matches)
            hashes_match("0xABZZ9999", "AB") -> True  (prefix matches)
            
        This is intentional for disambiguation workflows where nodes are 
        identified by prefix before full hash is known. If you need strict
        full-hash comparison, use normalize_hash() and compare directly.
    
    Args:
        hash_a: First hash or prefix
        hash_b: Second hash or prefix
        
    Returns:
        True if hashes match (considering prefix rules), False otherwise
        
    Examples:
        >>> # Exact full hash match
        >>> hashes_match("0xABCD1234", "0xABCD1234")
        True
        >>> hashes_match("0xABCD1234", "ABCD1234")  # Case/format normalized
        True
        
        >>> # Prefix matching (either side can be prefix)
        >>> hashes_match("0xABCD1234", "AB")  # Full vs prefix
        True
        >>> hashes_match("AB", "0xABCD1234")  # Prefix vs full
        True
        >>> hashes_match("AB", "AB")          # Prefix vs prefix
        True
        
        >>> # Non-matching
        >>> hashes_match("AB", "CD")
        False
        >>> hashes_match("0xABCD1234", "0xEFGH5678")
        False
    """
    if not hash_a or not hash_b:
        return False
    
    norm_a = normalize_hash(hash_a)
    norm_b = normalize_hash(hash_b)
    
    # Exact match
    if norm_a == norm_b:
        return True
    
    # Prefix match (either could be a prefix)
    prefix_a = get_prefix(norm_a)
    prefix_b = get_prefix(norm_b)
    
    # If one is just a prefix (2 chars), compare prefixes
    len_a = len(norm_a) - 2  # Exclude "0x"
    len_b = len(norm_b) - 2
    
    if len_a == 2 or len_b == 2:
        return prefix_a == prefix_b
    
    return False


def sanitize_for_sql(hash_str: Optional[str]) -> str:
    """
    Sanitize hash string for safe SQL usage.
    
    Removes any characters that aren't valid hex or prefix characters.
    Use this before constructing SQL queries with hash values.
    
    Args:
        hash_str: Hash string to sanitize
        
    Returns:
        Sanitized hash string safe for SQL, or empty string
        
    Examples:
        >>> sanitize_for_sql("0xABCD1234")
        '0xABCD1234'
        >>> sanitize_for_sql("!@#%^&*")
        ''
    """
    if not hash_str:
        return ""
    
    h = str(hash_str).strip().upper()
    
    # Remove 0x prefix for processing
    had_prefix = h.startswith("0X")
    if had_prefix:
        h = h[2:]
    
    # Keep only valid hex characters
    sanitized = ''.join(c for c in h if c in '0123456789ABCDEF')
    
    if not sanitized:
        return ""
    
    return f"0x{sanitized}"
