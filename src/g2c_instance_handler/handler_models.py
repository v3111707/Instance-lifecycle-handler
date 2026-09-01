import logging
from dataclasses import dataclass
from typing import Callable


class PermanentError(Exception):
    """The message can never succeed: no retry, straight to the failed queue.

    Retrying is pointless — the defect is in the message itself, not in the
    CMDB or the network — and it only delays the failed-queue publish.
    """

class MessageContractError(PermanentError):
    """The message violates the schema agreed with the provider."""

class _CmdbException(PermanentError):
    """A CMDB lookup/write failed in a way from which this message cannot recover."""


@dataclass(frozen=True)
class CreateContext:
    """Everything ``create_instances`` parsed out of one message, built once."""

    message: dict
    instance: dict
    host_networks: list
    hostname: str
    dc_name: str
    instance_id: str
    region_id: str
    project_id: str
    cmdb_tags: dict
    resource_cloud: dict
    cap_tenant: dict | None
    ready_state: str
    logger: logging.Logger


@dataclass(frozen=True)
class FieldStep:
    """One server field: what to write, and what failing to write it means.

    ``resolve`` is a plain ``(handler, CreateContext)`` function — no async, no
    CMDB access — and must not raise on well-formed input.
    """

    name: str
    resolve: Callable
    mark_state_error: bool = True
    include_in_status_details: bool = True


@dataclass(frozen=True)
class FieldWrite:
    """One resolved field write, ready to batch and ready to replay."""

    field: str
    mutation: dict
    mark_state_error: bool = True
    include_in_status_details: bool = True


class FieldErrors:
    """Accumulator for per-field failures.

    ``mark_state_error`` decides whether the final state becomes ``error``;
    ``include_in_status_details`` whether the message reaches ``status_details``.
    """

    __slots__ = ('_entries',)

    def __init__(self):
        self._entries = []

    def add(self, field, message, *, mark_state_error=True, include_in_status_details=True):
        self._entries.append((field, message, mark_state_error, include_in_status_details))

    @property
    def failed(self):
        """Was any *state-error* failure recorded?"""
        return any(mark_state_error for _, _, mark_state_error, _ in self._entries)

    @property
    def status_details(self):
        return '\n'.join(
            message for _, message, _, include_in_status_details in self._entries
            if include_in_status_details
        )

    @property
    def fields(self):
        """Every recorded failure's field name, in order. Diagnostics only."""
        return [field for field, _, _, _ in self._entries]


@dataclass
class HostNetwork:
    """One network card of the host being created."""

    order: int | None
    addresses: list
    name: str | None = None
    mac: str | None = None
    source: str = 'address'
    #: The order this card claims is already held by a card with another mac, so
    #: no bond number can be derived from it.
    placeholder: bool = False
