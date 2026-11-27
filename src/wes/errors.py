"""Exceptions shared by the engine, the asset catalog and the API."""

from __future__ import annotations


class NotFound(Exception):
    """The thing does not exist, or the caller's tenant may not see it."""


class Conflict(Exception):
    """The request is valid but the current state does not allow it."""


class Invalid(ValueError):
    """The request is malformed or breaks a rule (maps to HTTP 422)."""
