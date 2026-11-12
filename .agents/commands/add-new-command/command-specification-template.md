# $COMMAND_NAME command specification

## Supported version

Add supported Redis version here. For example: Redis >= 6.2.0

## Command description

Add a description of the command here.

## Command API

Specify an API for the command in the format that official docs uses. For example:

```
$COMMAND_NAME $key $member [NX|XX] [CH] [INCR]
```

## Redis-CLI examples

Add relevant Redis-CLI examples here.

## Command metadata

Paste the output of `COMMAND INFO $COMMAND_NAME` from a server that ships the command. It is
the source for the command's record in `_STATIC_COMMAND_METADATA`
(`redis/commands/metadata.py`): the command flags, the `request_policy:` / `response_policy:`
/ `nondeterministic_output` / `dont_cache` tips and the key specifications. For a container
command, include the nested subcommands.

## Test plan

Specify how you want to test the command in terms of integration testing. For example:

- Test only with required arguments, assert that single value returned
- Test with required arguments and optional XX modifier, ensure that 1 returned
- ...
