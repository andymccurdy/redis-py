from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from redis.commands.metadata import (
    CommandMetadata,
    CommandMetadataRecordsCache,
    CommandPolicies,
    PolicyRecords,
    RequestPolicy,
    ResponsePolicy,
)
from redis.exceptions import IncorrectPolicyType, RedisError, ResponseError
from redis.utils import deprecated_function, str_if_bytes

if TYPE_CHECKING:
    from redis.asyncio.cluster import ClusterNode

# The record type ``_build_command_records`` builds per command: policies or full metadata.
# Constrained to the two, rather than left open, so a ``to_record`` that builds anything else
# is a type error at the call site.
_RecordT = TypeVar("_RecordT", CommandPolicies, CommandMetadata)

# Re-exported for backwards compatibility: these types used to be defined here and are
# now owned by ``redis.commands.metadata``. Import them from their new home instead.
__all__ = [
    "AbstractCommandsParser",
    "AsyncCommandsParser",
    "CommandPolicies",
    "CommandsParser",
    "PolicyRecords",
    "RequestPolicy",
    "ResponsePolicy",
]


class AbstractCommandsParser:
    def _get_pubsub_keys(self, *args):
        """
        Get the keys from pubsub command.
        Although PubSub commands have predetermined key locations, they are not
        supported in the 'COMMAND's output, so the key positions are hardcoded
        in this method
        """
        if len(args) < 2:
            # The command has no keys in it
            return None
        args = [str_if_bytes(arg) for arg in args]
        command = args[0].upper()
        keys = None
        if command == "PUBSUB":
            # the second argument is a part of the command name, e.g.
            # ['PUBSUB', 'NUMSUB', 'foo'].
            pubsub_type = args[1].upper()
            if pubsub_type in ["CHANNELS", "NUMSUB", "SHARDCHANNELS", "SHARDNUMSUB"]:
                keys = args[2:]
        elif command in ["SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE"]:
            # format example:
            # SUBSCRIBE channel [channel ...]
            keys = list(args[1:])
        elif command in ["PUBLISH", "SPUBLISH"]:
            # format example:
            # PUBLISH channel message
            keys = [args[1]]
        return keys

    def parse_subcommand(self, command, **options):
        return _parse_subcommand(command)


class CommandsParser(AbstractCommandsParser):
    """
    Parses Redis commands to get command keys.
    COMMAND output is used to determine key locations.
    Commands that do not have a predefined key location are flagged with
    'movablekeys', and these commands' keys are determined by the command
    'COMMAND GETKEYS'.
    """

    def __init__(self, redis_connection):
        self.commands = {}
        self.redis_connection = redis_connection
        self.initialize(self.redis_connection)

    def initialize(self, r):
        commands = r.command()
        uppercase_commands = []
        for cmd in commands:
            if any(x.isupper() for x in cmd):
                uppercase_commands.append(cmd)
        for cmd in uppercase_commands:
            commands[cmd.lower()] = commands.pop(cmd)
        self.commands = commands

    # As soon as this PR is merged into Redis, we should reimplement
    # our logic to use COMMAND INFO changes to determine the key positions
    # https://github.com/redis/redis/pull/8324
    def get_keys(self, redis_conn, *args):
        """
        Get the keys from the passed command.

        NOTE: Due to a bug in redis<7.0, this function does not work properly
        for EVAL or EVALSHA when the `numkeys` arg is 0.
         - issue: https://github.com/redis/redis/issues/9493
         - fix: https://github.com/redis/redis/pull/9733

        So, don't use this function with EVAL or EVALSHA.
        """
        if len(args) < 2:
            # The command has no keys in it
            return None

        cmd_name = args[0].lower()
        if cmd_name not in self.commands:
            # try to split the command name and to take only the main command,
            # e.g. 'memory' for 'memory usage'
            cmd_name_split = cmd_name.split()
            cmd_name = cmd_name_split[0]
            if cmd_name in self.commands:
                # save the split command to args
                args = cmd_name_split + list(args[1:])
            else:
                # We'll try to reinitialize the commands cache, if the engine
                # version has changed, the commands may not be current
                self.initialize(redis_conn)
                if cmd_name not in self.commands:
                    raise RedisError(
                        f"{cmd_name.upper()} command doesn't exist in Redis commands"
                    )

        command = self.commands.get(cmd_name)
        if "movablekeys" in command["flags"]:
            keys = self._get_moveable_keys(redis_conn, *args)
        elif "pubsub" in command["flags"] or command["name"] == "pubsub":
            keys = self._get_pubsub_keys(*args)
        else:
            if (
                command["step_count"] == 0
                and command["first_key_pos"] == 0
                and command["last_key_pos"] == 0
            ):
                is_subcmd = False
                if "subcommands" in command:
                    subcmd_name = f"{cmd_name}|{args[1].lower()}"
                    for subcmd in command["subcommands"]:
                        if str_if_bytes(subcmd[0]) == subcmd_name:
                            command = self.parse_subcommand(subcmd)

                            if command["first_key_pos"] > 0:
                                is_subcmd = True

                # The command doesn't have keys in it
                if not is_subcmd:
                    return None
            last_key_pos = command["last_key_pos"]
            if last_key_pos < 0:
                last_key_pos = len(args) - abs(last_key_pos)
            keys_pos = list(
                range(command["first_key_pos"], last_key_pos + 1, command["step_count"])
            )
            keys = [args[pos] for pos in keys_pos]

        return keys

    def _get_moveable_keys(self, redis_conn, *args):
        """
        NOTE: Due to a bug in redis<7.0, this function does not work properly
        for EVAL or EVALSHA when the `numkeys` arg is 0.
         - issue: https://github.com/redis/redis/issues/9493
         - fix: https://github.com/redis/redis/pull/9733

        So, don't use this function with EVAL or EVALSHA.
        """
        # The command name should be split into separate arguments,
        # e.g. 'MEMORY USAGE' will be split into ['MEMORY', 'USAGE']
        pieces = args[0].split() + list(args[1:])
        try:
            keys = redis_conn.execute_command("COMMAND GETKEYS", *pieces)
        except ResponseError as e:
            message = e.__str__()
            if (
                "Invalid arguments" in message
                or "The command has no key arguments" in message
            ):
                return None
            else:
                raise e
        return keys

    @deprecated_function(
        version="8.2.0",
        reason="Use get_commands_metadata_cache() instead.",
    )
    def get_command_policies(self) -> PolicyRecords:
        """
        Retrieve and process the command policies for all commands and subcommands.

        DEPRECATED: use :meth:`get_commands_metadata_cache` instead. Nothing in this library calls this
        method any more, and it will be removed in a future release. A metadata resolver keeps a
        compatibility shim for an object that serves only this method, because it was the whole
        contract in 7.1.0; that shim is not an invitation to write one. To decide routing
        yourself, implement the public ``PolicyResolver`` ABC instead.

        This method traverses through commands and subcommands, extracting policy details
        from associated data structures and constructing a dictionary of commands with their
        associated policies. It supports nested data structures and handles both main commands
        and their subcommands.

        This is the routing view of :meth:`get_commands_metadata_cache`: the same traversal of the
        same reply, keyed the same way, projected down to the two policies the cluster client
        routes by. A caller that also needs the client-side-caching metadata asks for the
        metadata records instead.

        Returns:
            PolicyRecords: A collection of commands and subcommands associated with their
            respective policies.

        Raises:
            IncorrectPolicyType: If an invalid policy type is encountered during policy extraction.
        """
        return _build_policy_records(self.commands)

    def get_commands_metadata_cache(self) -> CommandMetadataRecordsCache:
        """
        Retrieve and process the metadata records cache for all commands and subcommands.

        This method normalizes the command flags, command tips and key metadata of the
        ``COMMAND`` output into metadata records, keyed the same way as the policy records
        that the deprecated ``get_command_policies`` returns. The routing policies each
        record carries are the ones that method resolves, so both views of the same reply
        stay in step.

        Returns:
            CommandMetadataRecordsCache: A collection of commands and subcommands
            associated with their respective metadata.

        Raises:
            IncorrectPolicyType: If an invalid policy type is encountered during policy extraction.
        """
        return _build_commands_metadata_cache(self.commands)


class AsyncCommandsParser(AbstractCommandsParser):
    """
    Parses Redis commands to get command keys.

    COMMAND output is used to determine key locations.
    Commands that do not have a predefined key location are flagged with 'movablekeys',
    and these commands' keys are determined by the command 'COMMAND GETKEYS'.

    NOTE: Due to a bug in redis<7.0, this does not work properly
    for EVAL or EVALSHA when the `numkeys` arg is 0.
     - issue: https://github.com/redis/redis/issues/9493
     - fix: https://github.com/redis/redis/pull/9733

    So, don't use this with EVAL or EVALSHA.
    """

    __slots__ = ("commands", "node")

    def __init__(self) -> None:
        self.commands: Dict[str, Union[int, Dict[str, Any]]] = {}

    async def initialize(self, node: Optional["ClusterNode"] = None) -> None:
        if node:
            self.node = node

        commands = await self.node.execute_command("COMMAND")
        self.commands = {cmd.lower(): command for cmd, command in commands.items()}

    # As soon as this PR is merged into Redis, we should reimplement
    # our logic to use COMMAND INFO changes to determine the key positions
    # https://github.com/redis/redis/pull/8324
    async def get_keys(self, *args: Any) -> Optional[Tuple[str, ...]]:
        """
        Get the keys from the passed command.

        NOTE: Due to a bug in redis<7.0, this function does not work properly
        for EVAL or EVALSHA when the `numkeys` arg is 0.
         - issue: https://github.com/redis/redis/issues/9493
         - fix: https://github.com/redis/redis/pull/9733

        So, don't use this function with EVAL or EVALSHA.
        """
        if len(args) < 2:
            # The command has no keys in it
            return None

        cmd_name = args[0].lower()
        if cmd_name not in self.commands:
            # try to split the command name and to take only the main command,
            # e.g. 'memory' for 'memory usage'
            cmd_name_split = cmd_name.split()
            cmd_name = cmd_name_split[0]
            if cmd_name in self.commands:
                # save the split command to args
                args = cmd_name_split + list(args[1:])
            else:
                # We'll try to reinitialize the commands cache, if the engine
                # version has changed, the commands may not be current
                await self.initialize()
                if cmd_name not in self.commands:
                    raise RedisError(
                        f"{cmd_name.upper()} command doesn't exist in Redis commands"
                    )

        command = self.commands.get(cmd_name)
        if "movablekeys" in command["flags"]:
            keys = await self._get_moveable_keys(*args)
        elif "pubsub" in command["flags"] or command["name"] == "pubsub":
            keys = self._get_pubsub_keys(*args)
        else:
            if (
                command["step_count"] == 0
                and command["first_key_pos"] == 0
                and command["last_key_pos"] == 0
            ):
                is_subcmd = False
                if "subcommands" in command:
                    subcmd_name = f"{cmd_name}|{args[1].lower()}"
                    for subcmd in command["subcommands"]:
                        if str_if_bytes(subcmd[0]) == subcmd_name:
                            command = self.parse_subcommand(subcmd)

                            if command["first_key_pos"] > 0:
                                is_subcmd = True

                # The command doesn't have keys in it
                if not is_subcmd:
                    return None
            last_key_pos = command["last_key_pos"]
            if last_key_pos < 0:
                last_key_pos = len(args) - abs(last_key_pos)
            keys_pos = list(
                range(command["first_key_pos"], last_key_pos + 1, command["step_count"])
            )
            keys = [args[pos] for pos in keys_pos]

        return keys

    async def _get_moveable_keys(self, *args: Any) -> Optional[Tuple[str, ...]]:
        try:
            keys = await self.node.execute_command("COMMAND GETKEYS", *args)
        except ResponseError as e:
            message = e.__str__()
            if (
                "Invalid arguments" in message
                or "The command has no key arguments" in message
            ):
                return None
            else:
                raise e
        return keys

    @deprecated_function(
        version="8.2.0",
        reason="Use get_commands_metadata_cache() instead.",
    )
    async def get_command_policies(self) -> PolicyRecords:
        """
        Retrieve and process the command policies for all commands and subcommands.

        DEPRECATED: use :meth:`get_commands_metadata_cache` instead. Nothing in this library calls this
        method any more, and it will be removed in a future release. A metadata resolver keeps a
        compatibility shim for an object that serves only this method, because it was the whole
        contract in 7.1.0; that shim is not an invitation to write one. To decide routing
        yourself, implement the public ``PolicyResolver`` ABC instead.

        This method traverses through commands and subcommands, extracting policy details
        from associated data structures and constructing a dictionary of commands with their
        associated policies. It supports nested data structures and handles both main commands
        and their subcommands.

        This is the routing view of :meth:`get_commands_metadata_cache`: the same traversal of the
        same reply, keyed the same way, projected down to the two policies the cluster client
        routes by. A caller that also needs the client-side-caching metadata asks for the
        metadata records instead.

        Returns:
            PolicyRecords: A collection of commands and subcommands associated with their
            respective policies.

        Raises:
            IncorrectPolicyType: If an invalid policy type is encountered during policy extraction.
        """
        return _build_policy_records(self.commands)

    async def get_commands_metadata_cache(self) -> CommandMetadataRecordsCache:
        """
        Retrieve and process the metadata records cache for all commands and subcommands.

        This method normalizes the command flags, command tips and key metadata of the
        ``COMMAND`` output into metadata records, keyed the same way as the policy records
        that the deprecated ``get_command_policies`` returns. The routing policies each
        record carries are the ones that method resolves, so both views of the same reply
        stay in step.

        Returns:
            CommandMetadataRecordsCache: A collection of commands and subcommands
            associated with their respective metadata.

        Raises:
            IncorrectPolicyType: If an invalid policy type is encountered during policy extraction.
        """
        return _build_commands_metadata_cache(self.commands)


# =============================================================================
# Private helpers
# =============================================================================
def _parse_subcommand(command: Any) -> Dict[str, Any]:
    """
    Parse a single entry of a command's ``subcommands`` field into a details dict
    with the same shape as a top-level ``COMMAND`` entry.
    """
    cmd_dict = {}
    cmd_name = str_if_bytes(command[0])
    cmd_dict["name"] = cmd_name
    cmd_dict["arity"] = int(command[1])
    cmd_dict["flags"] = [str_if_bytes(flag) for flag in command[2]]
    cmd_dict["first_key_pos"] = command[3]
    cmd_dict["last_key_pos"] = command[4]
    cmd_dict["step_count"] = command[5]
    if len(command) > 7:
        cmd_dict["tips"] = command[7]
        cmd_dict["key_specifications"] = command[8]
        cmd_dict["subcommands"] = command[9]
    return cmd_dict


def _is_keyless_command(
    commands: Dict[str, Any],
    command_name: str,
    subcommand_name: Optional[str] = None,
) -> bool:
    """
    Determines whether a given command or subcommand is considered "keyless".

    A keyless command does not operate on specific keys, which is determined based
    on the first key position in the command or subcommand details. If the command
    or subcommand's first key position is zero or negative, it is treated as keyless -
    unless it is flagged ``movablekeys`` and its key specs report a key argument. A
    ``movablekeys`` command reports ``first_key_pos == 0`` because its keys are found by
    scanning the arguments, so only the key specs say that it takes keys at all.

    Parameters:
        commands: Dict[str, Any]
            The parsed ``COMMAND`` output to look the command up in.
        command_name: str
            The name of the command to check.
        subcommand_name: Optional[str], default=None
            The name of the subcommand to check, if applicable. If not provided,
            the check is performed only on the command.

    Returns:
        bool
            True if the specified command or subcommand is considered keyless,
            False otherwise.

    Raises:
        ValueError
            If the specified subcommand is not found within the command or the
            specified command does not exist in the available commands.
    """
    if subcommand_name:
        for subcommand in commands.get(command_name)["subcommands"]:
            if str_if_bytes(subcommand[0]) == subcommand_name:
                return _is_keyless(_parse_subcommand(subcommand))
        raise ValueError(
            f"Subcommand {subcommand_name} not found in command {command_name}"
        )
    else:
        command_details = commands.get(command_name, None)
        if command_details is not None:
            return _is_keyless(command_details)

        raise ValueError(f"Command {command_name} not found in commands")


def _is_keyless(details: Dict[str, Any]) -> bool:
    """
    The keyless test of :func:`_is_keyless_command`, for one command's parsed details.

    ``first_key_pos`` alone decides a command that reports it, so a command whose key specs
    are all ``not_key`` - SPUBLISH's shard channel - keeps routing by that argument's slot.
    """
    if details["first_key_pos"] > 0:
        return False

    return not ("movablekeys" in details["flags"] and _has_key_argument(details))


# Slots of the two-element buffer ``_apply_policy_tips`` writes into. A list rather than a
# record, so one buffer can be reused for every command of a ``COMMAND`` reply instead of a
# policy object being allocated per command and thrown away.
_REQUEST_POLICY = 0
_RESPONSE_POLICY = 1


def _walk_tips(data: Any) -> Iterator[str]:
    """
    Recursively yield every tip string of a ``COMMAND`` reply fragment, decoding bytes.

    The one traversal both tip readers share, so the routing view and the metadata view
    cannot disagree about which structures a tip may be nested inside. That matters because a
    metadata record is the superset of a policy record: it carries the two routing policies
    plus the cacheability markers, so it has to read at least everything the policy view
    reads off the same field.

    A ``COMMAND`` reply reports a command's ``tips`` as a flat array, but the fragments
    :func:`_apply_policy_tips` is handed for a container subcommand are whole reply entries,
    whose nested arrays and maps have to be walked. Anything that is neither a string, an
    array nor a map carries no tip and is skipped.

    Args:
        data: The fragment to walk (can be list, dict, str, bytes, etc.)

    Yields:
        Each tip string found, in reply order, so that a caller keeping only one value per
        slot ends up with the last one the reply carried.
    """
    if isinstance(data, (str, bytes)):
        # Decode bytes to string if needed
        yield str_if_bytes(data)

    elif isinstance(data, list):
        # For lists, recursively process each element
        for item in data:
            yield from _walk_tips(item)

    elif isinstance(data, dict):
        # For dictionaries, recursively process each value
        for value in data.values():
            yield from _walk_tips(value)


def _apply_policy_tips(policy_pair: List[Any], data: Any) -> None:
    """
    Extract policies from nested data structures.

    Args:
        policy_pair: The ``[request_policy, response_policy]`` buffer to update in place.
            The last policy tip found for a slot wins.
        data: The data structure to search, walked by :func:`_walk_tips`.

    Raises:
        IncorrectPolicyType: If an invalid policy type is encountered.
    """
    for policy in _walk_tips(data):
        # Check if this is a policy string
        if policy.startswith("request_policy"):
            policy_type = policy.split(":")[1]

            try:
                policy_pair[_REQUEST_POLICY] = RequestPolicy(policy_type)
            except ValueError:
                raise IncorrectPolicyType(
                    f"Incorrect request policy type: {policy_type}"
                )

        elif policy.startswith("response_policy"):
            policy_type = policy.split(":")[1]

            try:
                policy_pair[_RESPONSE_POLICY] = ResponsePolicy(policy_type)
            except ValueError:
                raise IncorrectPolicyType(
                    f"Incorrect response policy type: {policy_type}"
                )


def _build_command_records(
    commands: Dict[str, Any],
    to_record: Callable[[Dict[str, Any], RequestPolicy, ResponsePolicy], _RecordT],
) -> Dict[str, Dict[str, _RecordT]]:
    """
    Traverse a parsed ``COMMAND`` reply, resolving the policies of every command, and build
    one record per command with ``to_record``.

    ``to_record`` is called once per command and once per container subcommand, with the
    command's details and its resolved policies. Policies start at the keyed or keyless
    defaults, chosen by whether the command takes keys, and are then overwritten by the
    ``request_policy`` and ``response_policy`` command tips.

    This is the single traversal behind both the policy records and the metadata records, so
    the two are keyed identically by construction and the tips of a reply are walked once no
    matter which view a caller asks for. The policies are handed over as plain enum members,
    so the caller that wants a ``CommandPolicies`` record builds one and the caller that
    wants a ``CommandMetadata`` record is not charged for one it would discard.

    Args:
        commands: The parsed ``COMMAND`` output to traverse.
        to_record: Builds the record for one command from its details and policies.

    Returns:
        The records of every command and subcommand, keyed by module name, then by command
        name.

    Raises:
        IncorrectPolicyType: If an invalid policy type is encountered during policy extraction.
    """
    command_records: Dict[str, Dict[str, _RecordT]] = {}
    # Reused by every command below: the policies are read out of it before the next command
    # resets it, so no per-command buffer is allocated.
    policy_pair: List[Any] = [
        RequestPolicy.DEFAULT_KEYLESS,
        ResponsePolicy.DEFAULT_KEYLESS,
    ]

    for command, details in commands.items():
        # Check whether the command has keys
        is_keyless = _is_keyless_command(commands, command)

        if is_keyless:
            policy_pair[_REQUEST_POLICY] = RequestPolicy.DEFAULT_KEYLESS
            policy_pair[_RESPONSE_POLICY] = ResponsePolicy.DEFAULT_KEYLESS
        else:
            policy_pair[_REQUEST_POLICY] = RequestPolicy.DEFAULT_KEYED
            policy_pair[_RESPONSE_POLICY] = ResponsePolicy.DEFAULT_KEYED

        module_name, command_name = _split_module_and_command(command)
        module_records = command_records.setdefault(module_name, {})

        tips = details.get("tips")
        subcommands = details.get("subcommands")

        # Process tips for the main command
        if tips:
            _apply_policy_tips(policy_pair, tips)

        module_records[command_name] = to_record(
            details, policy_pair[_REQUEST_POLICY], policy_pair[_RESPONSE_POLICY]
        )

        # Process subcommands
        if subcommands:
            for subcommand_details in subcommands:
                # Get the subcommand name (first element)
                subcmd_name = subcommand_details[0]
                if isinstance(subcmd_name, bytes):
                    subcmd_name = subcmd_name.decode()

                # Check whether the subcommand has keys
                is_keyless = _is_keyless_command(commands, command, subcmd_name)

                if is_keyless:
                    policy_pair[_REQUEST_POLICY] = RequestPolicy.DEFAULT_KEYLESS
                    policy_pair[_RESPONSE_POLICY] = ResponsePolicy.DEFAULT_KEYLESS
                else:
                    policy_pair[_REQUEST_POLICY] = RequestPolicy.DEFAULT_KEYED
                    policy_pair[_RESPONSE_POLICY] = ResponsePolicy.DEFAULT_KEYED

                # Container subcommands are keyed by their space-joined name, e.g.
                # ``memory usage``, which is the form ``execute_command`` receives.
                subcmd_name = subcmd_name.replace("|", " ")

                # Recursively extract policies from the rest of the subcommand details
                for subcommand_detail in subcommand_details[1:]:
                    _apply_policy_tips(policy_pair, subcommand_detail)

                module_records[subcmd_name] = to_record(
                    _parse_subcommand(subcommand_details),
                    policy_pair[_REQUEST_POLICY],
                    policy_pair[_RESPONSE_POLICY],
                )

    return command_records


def _to_command_policies(
    details: Dict[str, Any],
    request_policy: RequestPolicy,
    response_policy: ResponsePolicy,
) -> CommandPolicies:
    """Build the policy record for a single entry of a ``COMMAND`` reply."""
    return CommandPolicies(
        request_policy=request_policy, response_policy=response_policy
    )


def _build_policy_records(commands: Dict[str, Any]) -> PolicyRecords:
    """
    Retrieve and process the command policies for all commands and subcommands.

    This function traverses through commands and subcommands, extracting policy details
    from associated data structures and constructing a dictionary of commands with their
    associated policies. It supports nested data structures and handles both main commands
    and their subcommands.

    Args:
        commands: The parsed ``COMMAND`` output to build the policy records from.

    Returns:
        PolicyRecords: A collection of commands and subcommands associated with their
        respective policies.

    Raises:
        IncorrectPolicyType: If an invalid policy type is encountered during policy extraction.
    """
    return _build_command_records(commands, _to_command_policies)


def _split_module_and_command(command: str) -> Tuple[str, str]:
    """
    Split a name from a ``COMMAND`` reply into the ``(module, command)`` pair the record
    tables are keyed by. Non-module commands live under ``"core"``.
    """
    split_name = command.split(".")

    if len(split_name) > 1:
        return split_name[0], split_name[1]

    return "core", split_name[0]


def _key_spec_flags(key_spec: Any) -> Set[str]:
    """
    The flags of a single key specification.

    RESP3 reports a key spec as a map and RESP2 as a flat ``[name, value, ...]`` sequence,
    so both shapes are read here.
    """
    if isinstance(key_spec, dict):
        flags = key_spec.get(b"flags") or key_spec.get("flags") or ()
    else:
        flags = ()
        for index in range(0, len(key_spec) - 1, 2):
            if str_if_bytes(key_spec[index]) == "flags":
                flags = key_spec[index + 1]
                break

    return {str_if_bytes(flag) for flag in flags}


def _has_key_argument(details: Dict[str, Any]) -> bool:
    """
    Whether the command accepts at least one Redis key name argument.

    Key specs decide it whenever the server reports them, including for ``movablekeys``
    commands, whose keys are not discoverable from the legacy positions at all. A spec
    flagged ``not_key`` describes an argument that is not a key name, such as a shard
    pubsub channel, so a command whose every spec is ``not_key`` takes no keys.

    Only a server that reports no key specs falls back to the legacy positions.
    ``last_key_pos`` is never consulted: it is ``-1`` for variadic commands whose key
    arguments run to the end of the argument list, and must not disqualify one.
    """
    key_specs = details.get("key_specifications")

    if key_specs:
        return any("not_key" not in _key_spec_flags(spec) for spec in key_specs)

    return details.get("first_key_pos", 0) > 0 and details.get("step_count", 0) > 0


def _to_command_metadata(
    details: Dict[str, Any],
    request_policy: RequestPolicy,
    response_policy: ResponsePolicy,
) -> CommandMetadata:
    """
    Build the metadata record for a single entry of a ``COMMAND`` reply.

    ``details`` is either a top-level entry or a ``_parse_subcommand`` result. The routing
    policies are passed in rather than derived again, so a record always carries the same
    policies ``_build_policy_records`` resolves for the command.
    """
    flags = {str_if_bytes(flag) for flag in details.get("flags") or ()}
    # Walked rather than read as a flat sequence, through the same traversal the routing view
    # resolves its tips with: a metadata record is the superset of a policy record, so it must
    # not read less of the field than the policy view does.
    tips = set(_walk_tips(details.get("tips")))

    return CommandMetadata(
        request_policy=request_policy,
        response_policy=response_policy,
        is_readonly="readonly" in flags,
        is_blocking="blocking" in flags,
        has_key_argument=_has_key_argument(details),
        # Matched exactly: ``nondeterministic_output_order`` is a different tip, denoting
        # only that element order varies.
        has_nondeterministic_output="nondeterministic_output" in tips,
        is_script_runner="script_runner" in flags,
        is_dont_cache="dont_cache" in tips,
        # An empty tips list is a real answer; a missing tips key means the server is too
        # old to report tips at all, which makes the negative markers undetectable.
        has_complete_metadata="flags" in details and "tips" in details,
    )


def _build_commands_metadata_cache(
    commands: Dict[str, Any],
) -> CommandMetadataRecordsCache:
    """
    Retrieve and process the metadata records cache for all commands and subcommands.

    This function traverses through commands and subcommands, normalizing the command
    flags, command tips and key metadata of each into a metadata record. It shares its
    traversal with ``_build_policy_records``, so records are keyed the same way and carry
    the same policies, and a metadata resolver and a policy resolver agree on the same reply.

    Args:
        commands: The parsed ``COMMAND`` output to build the metadata records from.

    Returns:
        CommandMetadataRecordsCache: A collection of commands and subcommands associated
        with their respective metadata.

    Raises:
        IncorrectPolicyType: If an invalid policy type is encountered during policy extraction.
    """
    return _build_command_records(commands, _to_command_metadata)
