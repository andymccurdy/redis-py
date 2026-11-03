import pytest

from redis._parsers import AsyncCommandsParser
from redis.commands.metadata import RequestPolicy, ResponsePolicy
from tests.conftest import skip_if_server_version_gte, skip_if_server_version_lt
from tests.helpers import get_expected_command_policies


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
class TestAsyncCommandParser:
    @pytest.mark.asyncio
    async def test_movablekeys_commands_derive_the_keyed_defaults(self, r):
        """
        A ``movablekeys`` command reports ``first_key_pos`` 0, so the keyed default is
        derived from its key specs. A command whose only key spec is ``not_key`` keeps
        routing by its first argument's slot, as SPUBLISH must.
        """
        commands_parser = AsyncCommandsParser()
        await commands_parser.initialize(node=r.get_default_node())
        metadata = (await commands_parser.get_commands_metadata_cache())["core"]
        keyed = (RequestPolicy.DEFAULT_KEYED, ResponsePolicy.DEFAULT_KEYED)

        for name in ("zinter", "sintercard", "xread", "eval", "eval_ro", "fcall_ro"):
            policies = (metadata[name].request_policy, metadata[name].response_policy)
            assert policies == keyed, name
        assert metadata["spublish"].request_policy is RequestPolicy.DEFAULT_KEYED
        assert metadata["lastsave"].request_policy is RequestPolicy.DEFAULT_KEYLESS

    @pytest.mark.asyncio
    @skip_if_server_version_gte("8.5.240")
    async def test_get_command_policies(self, r):
        commands_parser = AsyncCommandsParser()
        await commands_parser.initialize(node=r.get_default_node())
        expected_command_policies = get_expected_command_policies()

        actual_policies = await commands_parser.get_command_policies()
        assert len(actual_policies) > 0

        for module_name, commands in expected_command_policies.items():
            for command, command_policies in commands.items():
                assert command in actual_policies[module_name]
                assert command_policies == [
                    command,
                    actual_policies[module_name][command].request_policy,
                    actual_policies[module_name][command].response_policy,
                ]

    @skip_if_server_version_lt("8.5.240")
    @pytest.mark.asyncio
    async def test_get_command_policies_json_debug_movablekeys_stays_keyed(self, r):
        """
        JSON.DEBUG is ``movablekeys`` from 8.5.240 on, so its key is only in the key specs
        and ``first_key_pos`` is 0. The keyed default is derived from those specs instead.
        """
        commands_parser = AsyncCommandsParser()
        await commands_parser.initialize(node=r.get_default_node())
        changes_in_defaults = {
            "json": {
                "debug": [
                    "debug",
                    RequestPolicy.DEFAULT_KEYED,
                    ResponsePolicy.DEFAULT_KEYED,
                ],
            },
        }
        expected_command_policies = get_expected_command_policies(changes_in_defaults)

        actual_policies = await commands_parser.get_command_policies()
        assert len(actual_policies) > 0

        for module_name, commands in expected_command_policies.items():
            for command, command_policies in commands.items():
                assert command in actual_policies[module_name]
                assert command_policies == [
                    command,
                    actual_policies[module_name][command].request_policy,
                    actual_policies[module_name][command].response_policy,
                ]
