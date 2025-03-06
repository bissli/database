"""
Common test utilities for all test modules.
"""


def assert_lower(string1, string2, message=None):
    """
    Assert that two strings are equal when converted to lowercase.

    Args:
        string1: First string to compare
        string2: Second string to compare
        message: Optional message to display on failure
    """
    assert string1.lower() == string2.lower(), message or f"Strings not equal in lowercase: '{string1}' != '{string2}'"


def assert_contains_lower(string_to_search, substring, message=None):
    """
    Assert that a string contains a substring, case-insensitive.

    Args:
        string_to_search: String to search in
        substring: Substring to search for
        message: Optional message to display on failure
    """
    assert substring.lower() in string_to_search.lower(), message or f"Substring not found in lowercase: '{substring}' not in '{string_to_search}'"
