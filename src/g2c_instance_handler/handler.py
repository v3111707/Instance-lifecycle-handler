"""Routes instance lifecycle messages to CMDB create/delete/resize/attach/detach handlers."""

import asyncio
import logging
import json
import random
import sentry_sdk
from sentry_sdk.scope import Scope as SentryScope
from cmdb_client import CmdbClient
from g2c_instance_handler.handler_models import (
    CreateContext,
    FieldErrors,
    FieldStep,
    FieldWrite,
    MessageContractError,
    _CmdbException,
)

from g2c_instance_handler.handler_utils import (
    build_host_networks,
    classify,
    get_dc_name,
    get_instances_type,
    get_hasura_code,
    ip_in_net_prefixes,
    next_bond_name,
    remove_sensitive_data,
    resolve_ip_type,
)
from g2c_instance_handler.observability import (
    instance_messages_create,
    instance_messages_delete,
    instance_messages_resize,
    instance_messages_attach,
    instance_messages_detach,
    instance_create_processing_duration,
    instance_delete_processing_duration,
    instance_resize_processing_duration,
    instance_attach_processing_duration,
    instance_detach_processing_duration,
)

IMPORTER_NAME = '[K8S]instance_handler_v2'
DEFAULT_RESPONSIBLE_NAME = 'example-team'
DEFAULT_COST_CENTER_CC = 'CC00000'
K8S_NODE_TAG = 'provisioner.example-org/clusterID'
CAP_TENANT_TAG = 'cap_tenant.name'
CMDB_FIELDS_WHITELIST = ['state', 'responsible', 'owner', 'role', 'project', 'task']

CMDB_DEFAULT_CLASS_KEYS = {
    'owner': 'name',
    'responsible': 'name',
    'datacenter': 'name',
    'tenant': 'tenant',
    'rails': 'code',
    'cost_center': 'cc',
    'default': 'name'
}

class InstanceHandler:
    """Processes instance lifecycle messages (create, delete, resize, attach,
    detach) by reading and writing the CMDB."""

    _default_cost_center_cc = DEFAULT_COST_CENTER_CC
    _cap_tenant_tag = CAP_TENANT_TAG
    _k8s_node_tag = K8S_NODE_TAG
    _default_responsible_name = DEFAULT_RESPONSIBLE_NAME
    _cmdb_fields_whitelist = CMDB_FIELDS_WHITELIST
    _cmdb_default_class_keys = CMDB_DEFAULT_CLASS_KEYS
    _logger_name = 'main.IH'
    _error_state = 'error'
    _ready_state = 'ready'
    _error_msg_template = 'Error while saving "{}" {}. \n'
    #: Handler-level attempts per CMDB operation, not retries: the initial
    #: attempt plus four. Deliberately small — it absorbs a brief blip, while a
    #: real outage is answered by the message-level retry. A class attribute so
    #: an instance built without ``__init__`` still has a usable budget.
    _cmdb_transport_attempts = 5
    _cmdb_retry_base = 1.0
    _cmdb_retry_jitter = 0.5

    def __init__(
            self,
            cmdb_url: str,
            cmdb_username: str,
            cmdb_password: str,
            max_workers=10,
            cmdb_transport_attempts: int | None = None
    ):
        """Authenticate to the CMDB and load the repositories this handler operates on."""
        self._logger = logging.getLogger(self._logger_name)
        if cmdb_transport_attempts is not None:
            self._cmdb_transport_attempts = cmdb_transport_attempts
        self._sentry_scope = SentryScope.get_current_scope()
        self._sentry_scope.set_tag("handler", 'instance')
        self._logger.info(f'Authenticating in {cmdb_url!r} with {cmdb_username!r} creds')
        self.cmdb_client = CmdbClient(
            url=cmdb_url,
            username=cmdb_username,
            password=cmdb_password,
            max_workers=max_workers,
            use_graphql=True
        )

        self.cmdb_client.introspect()

        self._server_repo = self.cmdb_client.repo_by_name('server')
        self._resource_cloud_repo = self.cmdb_client.repo_by_name('resource_cloud')
        self._cap_tenant_repo = self.cmdb_client.repo_by_name('cap_tenant')
        self._rails_repo = self.cmdb_client.repo_by_name('rails')
        self._datacenter_repo = self.cmdb_client.repo_by_name('datacenter')

        self._ip_repo = self.cmdb_client.repo_by_name('ip')
        self._network_endpoint_repo = self.cmdb_client.repo_by_name('network_endpoint')
        self._network_prefix_repo = self.cmdb_client.repo_by_name('network_prefix')

    async def process_message(self, message_body: dict):
        """Route one message to its handler based on ``action_type``, timing and counting the outcome.

        An unrecognized ``action_type`` is logged and otherwise ignored.
        """
        action_type = message_body.get('action_type')
        client_id = message_body.get('client_id')
        message_id = message_body.get('id')

        self._logger.info(f'{action_type=}, {client_id=} {message_id=}')
        self._sentry_scope.set_tag("action", action_type)

        match action_type:
            case 'create':
                with instance_create_processing_duration.time():
                    await self.create_instances(message_body)
                instance_messages_create.inc()
            case 'delete':
                with instance_delete_processing_duration.time():
                    await self.delete_instances(message_body)
                instance_messages_delete.inc()
            case 'resize':
                with instance_resize_processing_duration.time():
                    await self.resize_instances(message_body)
                instance_messages_resize.inc()
            case 'attach':
                with instance_attach_processing_duration.time():
                    await self.attach_to_instances(message_body)
                instance_messages_attach.inc()
            case 'detach':
                with instance_detach_processing_duration.time():
                    await self.detach_from_instances(message_body)
                instance_messages_detach.inc()
            case _:
                self._logger.debug(f'{action_type=} is not handled by the script')

    # -- actions: one per message action_type ---------------------------------

    async def create_instances(self, message_body: dict):
        """Handle a 'create' message: recreate the server row, write its
        resolved fields, and create its network endpoints and IPs.

        Does nothing if the message has no instance resource. A failure
        recreating the row aborts the whole message; per-field and
        per-network failures are instead accumulated and leave the server in
        ``error`` state with ``status_details`` set.
        """
        errors = FieldErrors()
        ctx = await self._build_create_context(message_body, errors)
        if ctx is None:
            return
        logger = ctx.logger

        # hostname *and* uuid: sibling keys in a Hasura bool_exp are ANDed, and a
        # bare value is normalized to {'_eq': value}. Contrast the explicit _or in
        # _remove_server and the uuid-only where in resize_instances.
        where = {'hostname': ctx.hostname, 'uuid': ctx.instance_id}

        try:
            new_server = await self._recreate_server_row(ctx)
        except _CmdbException as abort:
            logger.error(f'create_instances stopped: {abort}')
            return

        await self._write_create_fields(ctx, where, errors)

        # Create Network Objects
        await self._persist_net_endpoints(
            ctx, new_server_id=new_server['id'], errors=errors,
        )

        # Set State
        if errors.failed:
            data = {
                'state': self._error_state,
                'status_details': errors.status_details
            }
        else:
            data = {'state': ctx.ready_state}
        logger.info(f'Saving {data}')
        # ``record`` replaces the second, unguarded write this used to fall back
        # to. A data error here is permanent, so a repeat of the same write cannot
        # land either; a transport error re-raises and the message runs again.
        await self._update_server(data, where=where, errors=errors, field='state')
        logger.info(f'End!. failed={errors.failed}, fields={errors.fields}')

    async def delete_instances(self, message_body: dict):
        """Handle a 'delete' message: remove the server row (and its rails) matching the instance's uuid.

        Does nothing if the message has no instance resource.
        """
        self._logger.debug(f'remove server: {message_body}')
        instance = self._extract_resource(message_body, 'instance')
        if instance is None:
            self._logger.warning('Message doesn\'t have instances resources.')
            return
        # hostname = instance['search_field']
        instance_id = instance['resource_id']
        await self._remove_server(uuid=instance_id)

    async def resize_instances(self, message_body: dict):
        """Handle a 'resize' message: update the server's monthly payment
        and/or flavor, whichever are present in the message.

        Does nothing if the message has no instance resource.
        """
        instance = self._extract_resource(message_body, 'instance')
        if instance is None:
            self._logger.warning('Message doesn\'t have instances resources.')
            return

        hostname = instance['search_field']
        logger = logging.LoggerAdapter(self._logger, {'hostname': hostname})
        instance_id = instance['resource_id']
        region_id = message_body.get('region_id')
        # uuid only, and from ``search_field`` rather than ``name``: adding
        # hostname here would narrow the match against a different source.
        where = {'uuid': instance_id}

        # Set Server_monthly_payment
        if 'total_price' in message_body:
            data = {'server_monthly_payment': message_body['total_price'].get('price_per_month')}
            logger.info(f'Saving {data}')
            await self._update_server(data, where=where,
                                      raise_on_data_error=True,
                                      field='server_monthly_payment')

        # Set Flavor
        action_data = message_body.get('action_data', {})
        if action_data and action_data.get('flavor_id'):
            flavor_name = action_data.get('flavor_id')
            data = {'flavor': {'name': flavor_name, 'region_code': region_id}}
            logger.info(f'Saving {data}')
            await self._update_server(data, where=where,
                                      raise_on_data_error=True,
                                      field='flavor')

    async def attach_to_instances(self, message_body: dict):
        """Handle an 'attach' message: add a network endpoint and IP for one
        port to a server, reusing an existing endpoint by mac (or an unused
        placeholder) before creating a new one.

        Idempotent against redelivery: if the IP is already attached to the
        server, returns without changes. Does nothing if the message is
        missing an instance or port resource. A missing datacenter or
        network prefix degrades the new IP to no ``prefix_id``/``type``
        rather than failing the message.
        """
        instance = self._extract_resource(message_body, 'instance')
        port = self._extract_resource(message_body, 'port')
        network = self._extract_resource(message_body, 'network')

        if instance is None or port is None:
            self._logger.warning(f'Not enough data(instance or port) for processing the message: {instance=} {port=}  ')
            return

        net_prefix = None
        mac = None

        region_code = message_body['region_id']
        hostname_uuid = instance['resource_id']
        ip_addr = port['search_field']

        if isinstance(port.get('resource_body'), dict):
            mac = port['resource_body'].get('mac_address')

        server = await self._cmdb_call(
            self._server_repo.select_one,
            where={
                'uuid': hostname_uuid
            },
            columns=[
                'id',
                'hostname',
                {
                    'name': 'network_endpoints',
                    'columns': [
                        'id',
                        'name',
                        'notes',
                        'mac',
                        {
                            'name': 'ips',
                            'columns': [
                                'id',
                                'type',
                                'ip',
                            ]
                        }
                    ]
                }
            ],
            raise_on_data_error=True, field='server.select_one',
        )
        if not server:
            self._logger.warning(
                f'_attach_to_instances: server with uuid: {hostname_uuid!r} not found '
            )
            return

        # A redelivered attach used to create a second network_endpoint and a
        # second ip, with no constraint able to stop it. One select makes a
        # repeated attach an idempotent success. This covers sequential
        # redelivery, not two identical messages racing at the same time.
        already_attached = await self._find_attached_ip(ip_addr, hostname_uuid)
        if already_attached is not None:
            self._logger.warning(
                f'attach_to_instances: {ip_addr!r} is already attached to '
                f'{hostname_uuid!r}; this is a repeated delivery, nothing to do'
            )
            return

        # log: it degrades to the existing "dc not found" path, so the IP is
        # created without prefix_id and without type, exactly as a genuine miss.
        dc = await self._cmdb_call(
            self._datacenter_repo.select_one,
            where={
                'cloud_regions': {'region_code': region_code}
            },
            columns=[
                'name',
                'id'
            ],
            field='datacenter.select_one',
        )
        if not dc:
            self._logger.warning(
                f'attach_to_instances: dc with region_code: {region_code!r} not found'
            )
        else:
            net_prefix = await self._get_net_prefix_by_ip(
                ip_addr=ip_addr,
                dc_name=dc['name'],
            )
            if not net_prefix:
                self._logger.warning(
                    f'attach_to_instances: net_prefix for {ip_addr!r} '
                    f'dc = {dc["name"]!r} not found'
                )

        endpoints = server.get('network_endpoints') or []


        network_endpoint = next(
            (e for e in endpoints if mac and e.get('mac') == mac), None
        )
        if network_endpoint is None:
            network_endpoint = next(
                (e for e in endpoints if not e.get('ips') and not e.get('mac')),
                None,
            )

        if network_endpoint is not None:
            updates = {}
            if mac and not network_endpoint.get('mac'):
                updates['mac'] = mac
            if network and network.get('search_field') and not network_endpoint.get('notes'):
                updates['notes'] = network['search_field']

            if updates:
                await self._cmdb_call(
                    self._network_endpoint_repo.update,
                    where={'id': network_endpoint['id']}, mutation=updates,
                    field='network_endpoint.update',
                    raise_on_data_error=True
                )

            self._logger.info(
                f'attach_to_instances: reusing network_endpoint '
                f'{network_endpoint["name"]!r} (id={network_endpoint["id"]}, '
                f'{mac=}) on {hostname_uuid!r}'
            )
        else:
            recovered = {}
            name = next_bond_name(e.get('name') for e in endpoints)
            new_network_endpoint_data = {
                'server_id': server['id'],
                'importer': IMPORTER_NAME,
                'name': name,
            }
            if network and network.get('search_field'):
                new_network_endpoint_data['notes'] = network['search_field']
            if mac:
                new_network_endpoint_data['mac'] = mac

            self._logger.info(f'Create new network_endpoint: {new_network_endpoint_data}')

            # Both inserts abort: a missing endpoint makes the IP insert meaningless.

            network_endpoint = await self._cmdb_call(
                self._network_endpoint_repo.insert_one, new_network_endpoint_data,
                raise_on_data_error=True, field='network_endpoint.insert',
                subject=new_network_endpoint_data,
                # The name is unique per server now, so (server_id, name) is a
                # real natural key and the ip no longer has to stand in for it.
                selector=self._attach_endpoint_selector(
                    ip_addr, hostname_uuid, server['id'], name, recovered
                ),
            )

            assert network_endpoint is not None, 'raise_on_data_error=True rules out None'

            if 'ip' in recovered:
                self._logger.warning(
                    f'attach_to_instances: {ip_addr!r} turned up on {hostname_uuid!r} '
                    f'while this attach was running; nothing left to insert'
                )
                return

        new_ip_data = {
            'ip': ip_addr,
            'importer': IMPORTER_NAME,
            'provider': {'name': 'ExampleCloud'},
            'network_endpoint_id': network_endpoint['id'],
        }
        # The guard stays out here: without a prefix the IP is written with
        # neither prefix_id nor type, which is not the same as type 'int'.
        if net_prefix:
            new_ip_data['prefix_id'] = net_prefix['id']
            used_ip_types = {
                i['type'] for n in server['network_endpoints'] for i in n['ips']
            }
            new_ip_data['type'] = resolve_ip_type(net_prefix['type'], used_ip_types)

        self._logger.info(f'Create new ip: {new_ip_data}')
        await self._cmdb_call(
            self._ip_repo.insert_one, new_ip_data,
            raise_on_data_error=True, field='ip.insert', subject=new_ip_data,
            selector=self._attach_ip_selector(ip_addr, hostname_uuid),
        )


    async def detach_from_instances(self, message_body: dict):
        """Handle a 'detach' message: delete the IP matching
        ``action_data['ip_address']`` on this server, then delete its
        network endpoint if no IPs remain on it.

        Does nothing if the message has no instance resource or no
        ``ip_address`` in ``action_data``.
        """
        instance = self._extract_resource(message_body, 'instance')
        action_data = message_body.get('action_data')
        if not isinstance(action_data, dict) or 'ip_address' not in action_data:
            self._logger.warning(f'Detach message doesnt contain ip_address in the action_data:{action_data=}')
            return
        if instance is None:
            self._logger.warning('Message doesn\'t have instances resources.')
            return

        ip_addr = action_data['ip_address']
        host_uuid = instance['resource_id']
        hostname = instance['search_field']
        self._logger.info(f'Detach {ip_addr=} from {host_uuid=} ({hostname=})')

        # Detach writes no status_details, so a data error here has nothing to
        # record into and aborts straight to the dead-letter queue instead
        # (raise_on_data_error propagates _CmdbException uncaught, same as attach).
        ip_where = {
            'ip': {'_eq': ip_addr},
            'network_endpoint': {'server': {'uuid': {'_eq': host_uuid}}}
        }
        affected_ips = await self._cmdb_call(
            self._ip_repo.delete,
            where=ip_where,
            field='ip.delete',
            raise_on_data_error=True,
        )
        self._logger.info(f'_detach_to_instances: deleted {affected_ips} ip record(s) for {ip_where}')

        network_endpoint_where = {
            '_not': {
                'ips': {}
            },
            'server': {"uuid": {"_eq": host_uuid}}
        }
        affected_endpoints = await self._cmdb_call(
            self._network_endpoint_repo.delete,
            where=network_endpoint_where,
            field='network_endpoint.delete',
            raise_on_data_error=True,
        )
        self._logger.info(f'_detach_to_instances: deleted {affected_endpoints} network_endpoint(s) for {network_endpoint_where}')

    # -- create: reading the message ------------------------------------------

    def _extract_resources(self, message_body: dict, resource_type: str):
        """Every resource of ``resource_type``, or ``[]``."""
        resources = message_body.get('resources')
        if not isinstance(resources, list):
            raise MessageContractError(
                f"message id={message_body.get('id')!r}: 'resources' is required "
                f'and must be a list, got {type(resources).__name__}'
            )
        return [r for r in resources if r.get('resource_type') == resource_type]

    def _extract_resource(self, message_body: dict, resource_type: str):
        """The first resource of ``resource_type``, or ``None``."""
        resources = self._extract_resources(message_body, resource_type)
        if not resources:
            return None
        if len(resources) > 1:
            self._logger.warning(
                f'message id={message_body.get("id")!r} carries {len(resources)} '
                f'{resource_type!r} resources; using the first'
            )
        return resources[0]

    def _validate_create_contract(self, instance_data: dict, message_id: str | None):
        """The fields ``create`` cannot do without, checked before any CMDB call."""
        missing = []
        for field in ('name', 'id'):
            if instance_data.get(field) is None:
                missing.append(field)
        if missing:
            raise MessageContractError(
                f"message id={message_id!r}: the instance "
                f"resource_body is missing required field(s): {', '.join(missing)}"
            )

    async def _build_create_context(self, message_body: dict, errors: FieldErrors):
        """Parse one create message. ``None`` means "no instance resource"."""
        message_id = message_body.get('id')
        instance = self._extract_resource(message_body, 'instance')
        if instance is None:
            self._logger.warning('Message doesn\'t have instances resources.')
            return None

        instance_data = instance['resource_body']
        self._validate_create_contract(instance_data, message_id)

        client_id = message_body.get('client_id')
        project_id = message_body.get('project_id')
        region_id = message_body.get('region_id')

        hostname = instance_data['name']
        instance_id = instance_data['id']
        logger = logging.LoggerAdapter(self._logger, {'hostname': hostname})
        logger.debug(f'client_id: {client_id!r}, project_id: {project_id!r}, region_id: {region_id!r}')

        ports = [
            r['resource_body'] for r in self._extract_resources(message_body, 'port')
        ]
        host_networks = build_host_networks(
            instance_data.get('addresses', {}), ports, logger
        )

        # record, not log: a swallowed failure becomes {} and drops both
        # ``responsible`` and ``cost_center`` to their default rungs silently.
        resource_cloud = await self._cmdb_call(
            self._resource_cloud_repo.select_one,
            where={
                'project_id': str(project_id)
            },
            columns=[
                'responsible_id',
                {'name': 'account', 'columns': ['cost_center_id']}
            ],
            errors=errors, field='resource_cloud',
            raise_on_data_error=True,
        )
        if not resource_cloud:
            logger.warning(f'resource_cloud with project_id = {project_id!r} not found')
            resource_cloud = {}

        tags = {k: v for k, v in instance_data['metadata'].items()}
        cmdb_tags = {k[5:]: v for k, v in tags.items() if k.startswith('cmdb.')}

        is_k8s_node = self._k8s_node_tag in tags

        ready_state = cmdb_tags.pop('state', self._ready_state)
        cmdb_tags = {k: v for k, v in cmdb_tags.items() if not k.startswith('state.')}

        # see the tag-precedence ladder below
        cap_tenant = None
        if self._cap_tenant_tag in cmdb_tags:
            cap_tenant_name = cmdb_tags.pop(self._cap_tenant_tag)
            logger.info(f'Found cap_tenant.name tag: {cap_tenant_name!r}')
            # A miss already records an error, so a failed lookup lands in the
            # same place.
            resp = await self._cmdb_call(
                self._cap_tenant_repo.select_one,
                where={'name': cap_tenant_name},
                columns=['cost_center_id', 'owner_id'],
                errors=errors, field='cap_tenant',
                raise_on_data_error=True,
            )
            logger.debug(f'cap_tenant_repo.select_one resp: {resp}')
            if resp:
                cap_tenant = resp
                logger.info('K8S Node. Set is_cap_tenant = True')
            else:
                msg = f'Rabbit msg has tag cap_tenant.name, but cap_tenant class doesnt have record with name {cap_tenant_name!r}'
                errors.add('cap_tenant', msg)

        if not any((cap_tenant is not None, is_k8s_node)):
            cmdb_tags = {k: v for k, v in cmdb_tags.items()
                         if k.split('.', 1)[0] in self._cmdb_fields_whitelist}

        return CreateContext(
            message=message_body,
            instance=instance_data,
            host_networks=host_networks,
            hostname=hostname,
            dc_name=get_dc_name(hostname),
            instance_id=instance_id,
            region_id=region_id,
            project_id=project_id,
            cmdb_tags=cmdb_tags,
            resource_cloud=resource_cloud,
            cap_tenant=cap_tenant,
            ready_state=ready_state,
            logger=logger,
        )

    # -- create: resolving the server fields ----------------------------------

    def _is_simple_field(self, field):
        """``True`` if ``field`` is a plain column rather than a reference to another CMDB class."""
        if field in self._server_repo.refs:
            return False
        return True

    def tags2cmdb_field(self, data: dict):
        """Turn ``cmdb.*`` tag key-value pairs into a CMDB mutation dict.

        A key containing ``.`` (e.g. ``owner.name``) becomes a nested
        mutation. A key naming a plain column is written as-is. Anything
        else is treated as a reference to another class and wrapped using
        that class's default lookup key from ``_cmdb_default_class_keys``.
        """
        transformed_data = dict()
        for key, value in data.items():
            if '.' in key:
                repo_name, field_name = key.split('.', 1)
                transformed_data[repo_name] = {field_name: value}
            elif key and self._is_simple_field(key):
                transformed_data[key] = value
            else:
                field_name = self._cmdb_default_class_keys.get(key, self._cmdb_default_class_keys['default'])
                transformed_data[key] = {field_name: value}
        return transformed_data

    @staticmethod
    def _tag_fields(ctx: CreateContext, name: str):
        """The ``cmdb.*`` tags belonging to ``name``: exact key or ``name.`` prefix."""
        return {k: v for k, v in ctx.cmdb_tags.items()
                if k == name or k.startswith(name + '.')}

    def _resolve_dc(self, ctx: CreateContext):
        """Resolve ``dc`` from the hostname's datacenter prefix."""
        return {'dc': {'name': ctx.dc_name}}

    def _resolve_cloud_project(self, ctx: CreateContext):
        """Resolve ``cloud_resource_project_id`` from ``resource_cloud``, or ``None`` if it was not found."""
        if not ctx.resource_cloud:
            return None
        return {'cloud_resource_project_id': ctx.resource_cloud['id']}

    def _resolve_tenant(self, ctx: CreateContext):
        """Resolve ``tenant`` from ``project_id``."""
        return {'tenant': {'tenant_code': str(ctx.project_id)}}

    def _resolve_owner(self, ctx: CreateContext):
        """Resolve ``owner`` from ``cap_tenant``, then tags, then a default."""
        if ctx.cap_tenant is not None:
            return {'owner_id': ctx.cap_tenant.get('owner_id')}
        tags = self._tag_fields(ctx, 'owner')
        if tags:
            return self.tags2cmdb_field(tags)
        return {'owner': {'name': self._default_responsible_name}}

    def _resolve_responsible(self, ctx: CreateContext):
        """Resolve ``responsible`` from ``cap_tenant``, then tags, then ``resource_cloud``, then a default."""
        # The cap-tenant rung reads owner_id on purpose: cap_tenant has no
        # responsible of its own.
        if ctx.cap_tenant is not None:
            return {'responsible_id': ctx.cap_tenant.get('owner_id')}
        tags = self._tag_fields(ctx, 'responsible')
        if tags:
            return self.tags2cmdb_field(tags)
        if ctx.resource_cloud.get('responsible_id'):
            return {'responsible_id': ctx.resource_cloud.get('responsible_id')}
        return {'responsible': {'name': self._default_responsible_name}}

    def _resolve_flavor(self, ctx: CreateContext):
        """Resolve ``flavor`` from the instance's flavor name and region."""
        return {'flavor': {'name': ctx.instance['flavor']['original_name'],
                           'region_code': ctx.region_id}}

    def _resolve_payment(self, ctx: CreateContext):
        """Resolve ``server_monthly_payment`` from the message's ``total_price``, or ``None`` if absent."""
        total_price = ctx.message.get('total_price')
        if not total_price:
            return None
        return {'server_monthly_payment': total_price.get('price_per_month')}

    def _resolve_cost_center(self, ctx: CreateContext):
        """Resolve ``cost_center`` from ``cap_tenant``, then tags, then ``resource_cloud``'s account, then a default."""
        if ctx.cap_tenant is not None:
            return {'cost_center_id': ctx.cap_tenant.get('cost_center_id')}
        tags = self._tag_fields(ctx, 'cost_center')
        if tags:
            return self.tags2cmdb_field(tags)
        account = ctx.resource_cloud.get('account')
        if account and account.get('cost_center_id'):
            return {'cost_center_id': account['cost_center_id']}
        return {'cost_center': {'cc': self._default_cost_center_cc}}

    #: Order is the order the fields are written in. ``name`` doubles as the
    #: ``cmdb.*`` tag the step consumes.
    _CREATE_STEPS = (
        FieldStep('dc',                     _resolve_dc),
        FieldStep('cloud_resource_project', _resolve_cloud_project),
        FieldStep('tenant',                 _resolve_tenant,  mark_state_error=False),
        FieldStep('owner',                  _resolve_owner),
        FieldStep('responsible',            _resolve_responsible),
        FieldStep('flavor',                 _resolve_flavor,
                  mark_state_error=False, include_in_status_details=False),
        FieldStep('server_monthly_payment', _resolve_payment),
        FieldStep('cost_center',            _resolve_cost_center),
    )

    def _create_writes(self, ctx: CreateContext):
        """Every field write for one create, in the order they are written."""
        writes = []
        for step in self._CREATE_STEPS:
            mutation = step.resolve(self, ctx)
            if mutation is None:
                continue
            writes.append(FieldWrite(
                step.name, mutation,
                mark_state_error=step.mark_state_error,
                include_in_status_details=step.include_in_status_details,
            ))
        for tag, value in self._leftover_tags(ctx).items():
            writes.append(FieldWrite(tag, self.tags2cmdb_field({tag: value})))
        return writes

    def _merge_writes(self, writes, logger):
        """One mutation out of many, merged **shallowly**.

        Shallow on purpose: a deep merge would turn two one-key dicts for the same
        relation into a lookup matching both fields — a different query. Last wins,
        and the losing value is never resolved, hence the warning.
        """
        batch = {}
        owner = {}
        for write in writes:
            for key in write.mutation:
                if key in owner:
                    logger.warning(
                        f'both {owner[key]!r} and {write.field!r} write {key!r}; '
                        f'the earlier value is dropped without being validated'
                    )
                owner[key] = write.field
            batch |= write.mutation
        return batch

    def _leftover_tags(self, ctx: CreateContext):
        """Tags no step in the table consumed, written one update per tag."""
        handled = {step.name for step in self._CREATE_STEPS}
        return {k: v for k, v in ctx.cmdb_tags.items()
                if k.split('.', 1)[0] not in handled}

    # -- create: writing the rows ---------------------------------------------

    async def _recreate_server_row(self, ctx: CreateContext):
        """Remove whatever is registered under this hostname or uuid, insert a
        fresh minimal row, and return it.

        Both calls abort on a permanent failure: continuing would insert a second
        row for the same hostname. The delete-then-insert is also what makes a
        whole-message retry safe.
        """
        await self._remove_server(uuid=ctx.instance_id, hostname=ctx.hostname)

        raw_info = remove_sensitive_data(ctx.message.copy())
        new_server_data = {
            'hostname': ctx.hostname,
            'uuid': ctx.instance_id,
            'state': 'setup',
            'importer': IMPORTER_NAME,
            'type': get_instances_type(ctx.instance),
        }
        # Logged before raw_info is attached, so the line stays small.
        ctx.logger.info(f'Create new server {new_server_data}')
        new_server_data['raw_info'] = json.dumps(raw_info)

        async def by_hostname():
            # hostname is the unique column; uuid is not.
            return await self._server_repo.select_one(
                where={'hostname': ctx.hostname},
                columns=['id', 'hostname', 'uuid']
            )

        return await self._cmdb_call(
            self._server_repo.insert_one, new_server_data,
            raise_on_data_error=True, field='server.insert',
            subject=new_server_data, selector=by_hostname,
        )

    async def _write_create_fields(self, ctx: CreateContext, where: dict,
                                   errors: FieldErrors):
        """One batched update, replayed field by field if it fails permanently."""
        logger = ctx.logger
        writes = self._create_writes(ctx)
        batch = self._merge_writes(writes, logger)
        if not batch:
            raise MessageContractError(
                f"message id={ctx.message.get('id')!r}, hostname={ctx.hostname!r}: "
                f"unexpected anomaly —  no field resolved a mutation and no leftover cmdb tags remained — "
                f"nothing to write for this instance"
            )


        logger.info(f'Saving batch: {batch}')
        # ``log``, so the batch failure itself never reaches status_details: the
        # replay below produces the real, attributable diagnosis. A transport
        # failure never arrives here — ``_cmdb_call`` re-raised it, because
        # replaying eight fields against an unreachable CMDB multiplies the load
        # by eight and cannot succeed.
        rows = await self._update_server(batch, where=where, field='batch')
        if rows is not None:
            return

        logger.warning(
            'the batched server update failed permanently; replaying field by '
            'field so each failure is attributed to its own field'
        )
        for write in writes:
            logger.info(f'Saving {write.mutation}')
            await self._update_server(
                write.mutation, where=where, errors=errors, field=write.field,
                mark_state_error=write.mark_state_error,
                include_in_status_details=write.include_in_status_details,
            )

    async def _persist_net_endpoints(
            self,
            ctx: CreateContext,
            new_server_id: int,
            errors: FieldErrors,
    ):
        """Write one ``network_endpoint`` per card and one ``ip`` per address."""
        logger = ctx.logger
        # Starts empty and grows across the cards: nothing already in CMDB is
        # consulted, because create has just re-inserted the server row.
        used_ip_types = set()

        # Real cards own the number their order gives them; a placeholder takes
        # whatever is left over. Collected before the loop so a placeholder cannot
        # claim a number a later card in this same create still needs.
        taken_names = ['bond' + str(hn.order - 1) for hn in ctx.host_networks
                       if not hn.placeholder]

        for host_network in ctx.host_networks:
            # The name and the key that re-selects it are one decision.
            if host_network.placeholder:
                # The order this card clashed on belongs to another mac, so no
                # bond number comes from it — the lowest free one is used instead.
                # A constant name here collided with itself as soon as one server
                # had two placeholders, and with whatever attach wrote later.
                # The mac still finds this row again; it is always set, because a
                # differing mac is the only thing that makes a card a placeholder.
                name = next_bond_name(taken_names)
                taken_names.append(name)
                selector = self._endpoint_selector(new_server_id,
                                                   mac=host_network.mac)
            else:
                # mac_addr_order_num starts at 1, so bond numbering is order - 1.
                # An input contract, not a case to handle: an order of 0 yields
                # 'bond-1' and no guard is added. bondN is distinct per card, so
                # (server_id, name) re-selects exactly this row.
                name = 'bond' + str(host_network.order - 1)
                selector = self._endpoint_selector(new_server_id, name=name)

            new_network_endpoint_data = {
                'server_id': new_server_id,
                'importer': IMPORTER_NAME,
                'name': name,
            }
            # Omitted when absent, never written as None.
            if host_network.name is not None:
                new_network_endpoint_data["notes"] = host_network.name

            if host_network.mac is not None:
                new_network_endpoint_data["mac"] = host_network.mac

            logger.info(f'Create new network_endpoint: {new_network_endpoint_data}')
            network_endpoint = await self._cmdb_call(
                self._network_endpoint_repo.insert_one, new_network_endpoint_data,
                errors=errors, field='network_endpoint',
                subject=new_network_endpoint_data,
                selector=selector,
            )
            # A recorded failure returns None, where the try/except used to
            # ``continue``. Without this the next line would dereference None.
            if network_endpoint is None:
                logger.warning(f'network_endpoint create failed: {new_network_endpoint_data}')
                continue

            logger.debug(f'network_endpoint create resp: {network_endpoint}')
            for addr in host_network.addresses:
                net_prefix = await self._get_net_prefix_by_ip(
                    ip_addr=addr,
                    dc_name=ctx.dc_name,
                    errors=errors,
                )
                if not net_prefix:
                    msg = f'network_prefix for ip {addr!r} not found. Skipping ip creation'
                    logger.warning(msg)
                    errors.add('network_prefix', msg)
                    continue

                new_ip_data = {
                    'name': addr,
                    'ip': addr,
                    'importer': IMPORTER_NAME,
                    'provider': {'name': 'ExampleCloud'},
                    'network_endpoint_id': network_endpoint['id'],
                    "prefix_id": net_prefix['id']       ,
                }

                ip_type = resolve_ip_type(net_prefix['type'], used_ip_types)
                new_ip_data['type'] = ip_type
                used_ip_types.add(ip_type)
                logger.info(f'Create new ip: {new_ip_data}')
                ip_rec = await self._cmdb_call(
                    self._ip_repo.insert_one, new_ip_data,
                    errors=errors, field='ip',
                    subject=new_ip_data,
                    # The full constraint key: all three columns are set here,
                    # because a missing prefix skips the ip entirely.
                    selector=self._ip_selector(
                        addr, network_endpoint['id'], net_prefix['id']
                    ),
                )
                if ip_rec is not None:
                    logger.debug(f'ip_repo create resp: {ip_rec}')

    # -- cmdb: the retry and reporting engine ---------------------------------

    @staticmethod
    async def _sleep(delay):
        """Seam for the retry backoff, so a test can make it instant."""
        await asyncio.sleep(delay)

    def _backoff(self, attempt):
        """The delay in seconds before retry number ``attempt``: linear backoff plus jitter."""
        return self._cmdb_retry_base * attempt + random.uniform(
            0, self._cmdb_retry_jitter
        )

    async def _recover_insert(self, label, selector):
        """After an ambiguous transport failure on an insert, look for the row.

        A blind retry could duplicate it: the insert may have committed with only
        the reply lost. A failed re-select is itself ambiguous, so it is left to
        the caller rather than treated as "not there".
        """
        row = await selector()
        if row is not None:
            self._logger.info(
                f'{label}: the row is already present, so the insert landed and '
                f'only the reply was lost; reusing it instead of inserting again'
            )
        return row

    async def _cmdb_call(self, op, *args,
                         raise_on_data_error: bool = False,
                         attempts=None,
                         selector=None,
                         errors=None,
                         field=None,
                         subject=None,
                         mark_state_error=True,
                         include_in_status_details=True,
                         **kwargs):
        """Run one CMDB operation with retry, classification and reporting."""
        attempts = self._cmdb_transport_attempts if attempts is None else attempts
        attempts = max(1, attempts)
        label = field or getattr(op, '__qualname__', None) or repr(op)

        for attempt in range(1, attempts + 1):
            if selector is not None and attempt > 1:
                try:
                    recovered = await self._recover_insert(label, selector)
                except Exception as exc:
                    self._logger.warning(
                        f'{label}: recovery re-select failed on attempt '
                        f'{attempt}/{attempts}, so it is retried instead of '
                        f'the insert: {exc}'
                    )
                    if attempt >= attempts:
                        self._logger.error(
                            f'{label}: transport budget of {attempts} attempt(s) '
                            f'exhausted; leaving it to the message-level retry'
                        )
                        raise
                    await self._sleep(self._backoff(attempt))
                    continue
                if recovered is not None:
                    return recovered

            try:
                return await op(*args, **kwargs)
            except _CmdbException:
                raise
            except Exception as exc:
                kind = classify(exc)
                hasura_code = get_hasura_code(exc)
                if kind != 'transport':
                    msg = self._error_msg_template.format(
                        subject if subject is not None else label, exc
                    )

                    self._logger.error(
                        f'{label}: the CMDB operation failed ({kind}, hasura code: {hasura_code!r}); '
                        f'not retried, raise_on_data_error={raise_on_data_error}: {exc}'
                    )
                    if kind == 'unknown':
                        self._logger.exception(exc)

                    sentry_sdk.capture_event(
                        event={'level': 'warning', 'message': msg},
                    )

                    if errors is not None:
                        errors.add(label, msg, mark_state_error=mark_state_error,
                                   include_in_status_details=include_in_status_details)
                    if raise_on_data_error:
                        raise _CmdbException(msg)
                    return None

                self._logger.warning(
                    f'{label}: transport failure on attempt {attempt}/{attempts} '
                    f'(hasura code: {hasura_code!r}): {exc}'
                )
                if attempt >= attempts:
                    self._logger.error(
                        f'{label}: transport budget of {attempts} attempt(s) '
                        f'exhausted; leaving it to the message-level retry'
                    )
                    raise
                await self._sleep(self._backoff(attempt))

    # -- cmdb: operations and insert selectors --------------------------------

    async def _update_server(self, mutation: dict, *, where: dict,
                             errors=None, field=None,
                             mark_state_error=True, include_in_status_details=True,
                             raise_on_data_error=False):
        """``server.update`` plus the zero-rows check the library does not do."""
        rows = await self._cmdb_call(
            self._server_repo.update, where=where, mutation=mutation,
            errors=errors, field=field,
            subject=mutation, mark_state_error=mark_state_error,
            include_in_status_details=include_in_status_details,
            raise_on_data_error=raise_on_data_error,
        )
        # raise_on_data_error=True turns a data error into _CmdbException above,
        # so rows is never None here for that case. raise_on_data_error=False
        # means a data error already got logged inside _cmdb_call and rows came
        # back None — we're fine with that, nothing more to add.
        if rows == []:
            msg = f'server.update matched no rows: {where=} {mutation=}'
            self._logger.warning(msg)
            sentry_sdk.capture_event(event={'level': 'warning', 'message': msg})
        return rows

    async def _remove_server(
            self,
            uuid: str | None = '',
            hostname: str | None = ''
    ):
        """Delete every server row matching ``uuid`` or ``hostname``, and their ``rails``.

        Returns ``True`` if any server was found and deleted, ``False`` otherwise.
        """
        self._logger.info(f'Trying to delete record from the class server with uuid={uuid!r}, hostname={hostname!r}')
        # ``rails`` nested here instead of a per-server rails.select: it comes
        # back as a plain list of plain dicts (nothing nested is wrapped into a
        # deletable item), so each rail is deleted by condition, not by
        # rail.delete().
        columns = ['hostname', 'uuid', 'project', {'name': 'rails', 'columns': ['id']}]
        where = {'_or': []}
        if uuid:
            where['_or'].append({'uuid': {'_eq': uuid}})
        if hostname:
            where['_or'].append({'hostname': {'_eq': hostname}})
        # Every call here aborts on a permanent failure: continuing would insert
        # a second server row for the same hostname and uuid, because the old one
        # was never removed.
        servers = await self._cmdb_call(
            self._server_repo.select,
            where=where,
            columns=columns,
            raise_on_data_error=True, field='server.select',
        )
        if not servers:
            self._logger.warning('No servers found')
            return False

        for s in servers:
            for rail in s.get('rails') or []:
                self._logger.info(f'Deleting rail: {rail}')
                await self._cmdb_call(
                    self._rails_repo.delete, where={'id': rail['id']},
                    raise_on_data_error=True, field='rails.delete',
                )
            self._logger.warning(f'Removing server: {s}.')
            del_resp = await self._cmdb_call(s.delete, raise_on_data_error=True,
                                             field='server.delete')
            self._logger.debug(f'server.delete() resp: {del_resp}')
        return True

    async def _get_net_prefix_by_ip(self, ip_addr: str, dc_name: str, *,
                                    errors=None, field='network_prefix'):
        """The prefix whose network contains ``ip_addr``, or ``None``."""
        vlans = await self._cmdb_call(
            self._network_prefix_repo.select,
            where={
                'network': {'$is_null': False},
                'dc': {'name': dc_name}
            },
            columns=[
                'id',
                'network',
                'type',
                'vlan_id'
            ],
            errors=errors, field=field,
        )
        if vlans is None:
            return None
        return ip_in_net_prefixes(ip_addr, vlans)

    async def _find_attached_ip(self, ip_addr: str, host_uuid: str):
        """The IP row attached to that server, or ``None``.

        An attached IP is unique per server, which makes this the one reliable
        selector on the attach path: it needs no ``prefix_id``, which attach omits
        when the prefix lookup fails.
        """
        return await self._cmdb_call(
            self._ip_repo.select_one,
            where={
                'ip': {'_eq': ip_addr},
                'network_endpoint': {'server': {'uuid': {'_eq': host_uuid}}}
            },
            columns=[
                'id',
                'ip',
                {'name': 'network_endpoint', 'columns': ['id', 'name']}
            ],
            field='ip.select_one',
        )

    def _endpoint_selector(self, server_id, **key):
        """``(server_id, <natural key>)`` — a factory, so the loop's values are
        captured. ``name=`` for a real bondN, ``mac=`` for a placeholder whose
        name repeats across cards."""
        async def selector():
            return await self._network_endpoint_repo.select_one(
                where={'server_id': server_id, **key},
                columns=['id', 'name']
            )
        return selector

    def _ip_selector(self, ip_addr, network_endpoint_id, prefix_id):
        """Build a selector for an IP row by address, endpoint id, and prefix id."""
        async def selector():
            return await self._ip_repo.select_one(
                where={
                    'ip': ip_addr,
                    'network_endpoint_id': network_endpoint_id,
                    'prefix_id': prefix_id,
                },
                columns=['id', 'ip']
            )
        return selector

    def _attach_ip_selector(self, ip_addr, host_uuid):
        """Build a selector for the IP already attached to this server."""
        async def selector():
            return await self._find_attached_ip(ip_addr, host_uuid)
        return selector

    def _attach_endpoint_selector(self, ip_addr, host_uuid, server_id, name,
                                  recovered):
        """The endpoint to reuse after an ambiguous endpoint insert.

        Three distinguishable states: the ip is there — everything landed, and it
        is handed back through ``recovered`` so the caller skips its own insert;
        the endpoint is there under the name this attach picked — the insert
        landed and only the reply was lost; or nothing — retry.

        The second probe is ``(server_id, name)`` rather than "some address-less
        endpoint on the host": the caller now picks a name that is unique on the
        server, and the orphan it used to guess at is reused before the insert.
        """
        async def selector():
            existing = await self._find_attached_ip(ip_addr, host_uuid)
            if existing is not None:
                recovered['ip'] = existing
                return existing['network_endpoint']
            return await self._network_endpoint_repo.select_one(
                where={'server_id': server_id, 'name': name},
                columns=['id', 'name']
            )
        return selector
