"""Helpers with no CMDB access and no I/O: pure functions over the message."""

import re
from ipaddress import ip_address, ip_network
from cmdb_client.errors import CMDBError
from requests.exceptions import RequestException
from g2c_instance_handler.handler_models import HostNetwork

BOND_NAME_RE = re.compile(r'^bond(\d+)$')

def classify(exc):
    """``'transport'`` (retryable), ``'data'`` or ``'unknown'`` (both permanent).

    ``unknown`` exists because a CMDB 5xx can surface as a bare ``KeyError``.
    """
    if isinstance(exc, RequestException):
        return 'transport'
    if isinstance(exc, CMDBError):
        return 'data'
    return 'unknown'


def get_hasura_code(exc):
    """The Hasura error code: the first token of ``f"{code} at {path}: {msg}"``."""
    text = str(exc)
    return text.split(' ', 1)[0] if text else ''


def get_dc_name(hostname):
    """The datacenter name: ``hostname``'s prefix before the first ``'-'``, upper-cased."""
    return hostname.split('-', 1)[0].upper()


def get_instances_type(instance):
    """``'bm'`` if the instance's hostname starts with ``<word>-b``, else ``'cvm'``."""
    hostname = instance.get('name')
    if re.search(r'^\w+-b', hostname):
        return 'bm'
    else:
        return 'cvm'


def remove_sensitive_data(data: dict):
    """Clear ``data['action_data']`` in place and return ``data``."""
    data['action_data'] = None
    return data

def next_bond_name(existing_names):
    """The lowest free ``bondN`` among a server's endpoint names.

    The unique key is (loadbalancer_id, server_id, vlan_id, name), so a constant
    placeholder name collides with itself on the second card of one server.
    Lowest free rather than max+1: a detach gives its number back, and the
    numbering stays dense. Anything that is not ``bondN`` is ignored rather than
    rejected — nothing here owns what else may write to the column.
    """
    taken = set()
    for name in existing_names:
        match = BOND_NAME_RE.match(name or '')
        if match:
            taken.add(int(match.group(1)))
    number = 0
    while number in taken:
        number += 1
    return f'bond{number}'


def resolve_ip_type(prefix_type, used):
    """``'be'``, ``'fe'`` or ``'int'`` for one address.

    Does not touch ``used``: the caller owns the set and adds the result.
    """
    if prefix_type in ('be', 'fe') and prefix_type not in used:
        return prefix_type
    return 'int'


def ip_in_net_prefixes(ip, net_prefixes):
    """The first prefix in ``net_prefixes`` whose network contains ``ip``, or ``None``."""
    for p in net_prefixes:
        try:
            if ip_address(ip) in ip_network(p.get('network')):
                return p
        except Exception:
            pass
    return None


def build_host_networks(addresses: dict, ports: list, logger):
    """One ``HostNetwork`` per card, from the message's addresses and ports.

    Grouped by ``mac_addr_order_num``, keeping only ``fixed`` addresses. Ports
    are deduplicated against the addresses by IP, not by mac: on baremetal the
    two differ.
    """
    by_order = {}
    unordered = []
    placeholders = []

    for net_name, entries in (addresses or {}).items():
        card = HostNetwork(order=None, addresses=[], name=net_name)
        for entry in entries:
            if entry.get('OS-EXT-IPS:type') not in ['fixed']:
                continue
            card.addresses.append(entry['addr'])
            if 'OS-EXT-IPS-MAC:mac_addr' in entry:
                card.mac = entry['OS-EXT-IPS-MAC:mac_addr']
            if 'mac_addr_order_num' in entry:
                card.order = entry['mac_addr_order_num']

        if card.order is None:
            unordered.append(card)
            continue

        clash = by_order.get(card.order)
        if clash is None:
            by_order[card.order] = card
            continue


        shared = (f'networks {clash.name!r} and {card.name!r} share '
                  f'mac_addr_order_num {card.order}')
        if clash.mac and card.mac and clash.mac != card.mac:
            logger.warning(
                f'{shared} but carry different macs ({clash.mac!r} vs {card.mac!r}); '
                f'keeping {card.name!r} as a placeholder endpoint'
            )
            card.placeholder = True
            placeholders.append(card)
        else:
            # One mac, or one of them unknown: the same card seen twice.
            logger.warning(f'{shared}; merging them into one card')
            clash.addresses.extend(card.addresses)
            clash.mac = clash.mac or card.mac

    cards = sorted(by_order.values(), key=lambda c: c.order)
    cards.extend(placeholders)

    counter = max((c.order for c in cards), default=0)

    for card in unordered:
        counter += 1
        card.order = counter
        logger.warning(
            f'network {card.name!r} has no usable mac_addr_order_num; '
            f'continuing from {counter}'
        )
        cards.append(card)

    taken = {ip for card in cards for ip in card.addresses}
    for port in ports:
        fresh = [fixed_ip['ip_address'] for fixed_ip in port.get('fixed_ips', [])
                 if fixed_ip['ip_address'] not in taken]
        if not fresh:
            continue
        counter += 1
        cards.append(HostNetwork(order=counter, addresses=fresh,
                                 mac=port['mac_address'], source='port'))
        taken |= set(fresh)

    return cards
