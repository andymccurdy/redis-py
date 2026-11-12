# Contributing

## Introduction

We appreciate your interest in considering contributing to redis-py.
Community contributions mean a lot to us.

## Contributions we need

You may already know how you'd like to contribute, whether it's a fix for a bug you
encountered, or a new feature your team wants to use.

If you don't know where to start, consider improving
documentation, bug triaging, and writing tutorials are all examples of
helpful contributions that mean less work for you.

## AI-driven contributions

Redis-py defines a list of custom Claude commands (skills) that may simplify your contribution by enriching the agent's
context with necessary information and repository best practices. Commands are well-structured instructions for common
recurring tasks (e.g., adding support for a new Redis command API).

The list of available commands is available via:

**Claude CLI**

```
claude /skills
```

**Augment CLI**

```
auggie command list
```

## Your First Contribution

Unsure where to begin contributing? You can start by looking through
[help-wanted
issues](https://github.com/andymccurdy/redis-py/issues?q=is%3Aopen+is%3Aissue+label%3ahelp-wanted).

Never contributed to open source before? Here are a couple of friendly
tutorials:

-   <http://makeapullrequest.com/>
-   <http://www.firsttimersonly.com/>

## Getting Started

Here's how to get started with your code contribution:

1.  Create your own fork of redis-py
2.  Do the changes in your fork
3.  Create a virtualenv and install the development dependencies from the dev_requirements.txt file:
    ```
    python -m venv .venv
    source .venv/bin/activate
    pip install -r dev_requirements.txt
    pip install -e .[jwt]
    ```

4.  If you need a development environment, run `invoke devenv`. Note: this relies on docker-compose to build environments, and assumes that you have a version supporting [docker profiles](https://docs.docker.com/compose/profiles/).
5.  While developing, make sure the tests pass by running `invoke tests`
6.  If you like the change and think the project could use it, send a
    pull request

To see what else is part of the automation, run `invoke -l`

## The Development Environment

Running `invoke devenv` installs the development dependencies specified
in the dev_requirements.txt. It starts all of the dockers used by this
project, and leaves them running. These can be easily cleaned up with
`invoke clean`. NOTE: it is assumed that the user running these tests,
can execute docker and its various commands.

-   A master Redis node
-   A Redis replica node
-   Three sentinel Redis nodes
-   A redis cluster
-   An stunnel docker, fronting the master Redis node

The replica node, is a replica of the master node, using the
[leader-follower replication](https://redis.io/topics/replication)
feature.

The sentinels monitor the master node in a [sentinel high-availability
configuration](https://redis.io/topics/sentinel).

## Testing

Call `invoke tests` to run all tests, or `invoke all-tests` to run linters
tests as well. With the 'tests' and 'all-tests' targets, all Redis and
RedisCluster tests will be run.

It is possible to run only Redis client tests (with cluster mode disabled) by
using `invoke standalone-tests`; similarly, RedisCluster tests can be run by using
`invoke cluster-tests`.

Each run of tests starts and stops the various dockers required. Sometimes
things get stuck, an `invoke clean` can help.

## Linting

Call `invoke linters` to run linters without also running tests.  
Call `invoke linters-fix` to run linters and automatically fix issues.

## Documentation

If relevant, update the code documentation, via docstrings, or in `/docs`.

You can check how the documentation looks locally by running `invoke build-docs`
and loading the generated HTML files in a browser.

Historically there is a mix of styles in the docstrings, but the preferred way
of documenting code is by applying the
[Google style](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html).
Type hints should be added according to PEP484, and should not be repeated in
the docstrings.

### Docker Tips

Following are a few tips that can help you work with the Docker-based
development environment.

To get a bash shell inside of a container:

`$ docker run -it <service> /bin/bash`

Containers run a minimal Debian image that probably lacks tools you want
to use. To install packages, first get a bash session (see previous tip)
and then run:

`$ apt update && apt install <package>`

To follow logs from a development service, run the following from the
repository root:

`$ docker compose logs -f <service>`

For example, `docker compose logs -f redis` follows the standalone Redis service.
The service name comes from `docker-compose.yml`; `docker logs` instead expects a
container name or ID, such as `redis-standalone`.

### Troubleshooting

If you get any errors when running `make dev` or `make test`, make sure
that you are using supported versions of Docker.

Please try at least versions of Docker.

-   Docker 19.03.12

## How to Report a Bug

### Security Vulnerabilities

**NOTE**: If you find a security vulnerability, do NOT open an issue.

If you believe you have found a security vulnerability, to ensure proper
review and assessment, we kindly ask vulnerability reports be submitted
through our [Redis Vulnerability Disclosure
Program](https://redis.io/redis-responsible-vulnerability-disclosure/).
Alternatively reach out via email to
[security@redis.com](mailto:security@redis.com).

In order to determine whether you are dealing with a security issue, ask
yourself these two questions:

-   Can I access something that's not mine, or something I shouldn't
    have access to?
-   Can I disable something for other people?

If the answer to either of those two questions are *yes*, then you're
probably dealing with a security issue. Note that even if you answer
*no*  to both questions, you may still be dealing with a security
issue, so if you're unsure, report it through the
[Redis Vulnerability Disclosure
Program](https://redis.io/redis-responsible-vulnerability-disclosure/) or
email [us](mailto:security@redis.com).

### Everything Else

When filing an issue, make sure to answer these five questions:

1.  What version of redis-py are you using?
2.  What version of redis are you using?
3.  What did you do?
4.  What did you expect to see?
5.  What did you see instead?

## Suggest a feature or enhancement

If you'd like to contribute a new feature, make sure you check our
issue list to see if someone has already proposed it. Work may already
be underway on the feature you want or we may have rejected a
feature like it already.

If you don't see anything, open a new issue that describes the feature
you would like and how it should work.

## Code review process

The core team regularly looks at pull requests. We will provide
feedback as soon as possible. After receiving our feedback, please respond
within two weeks. After that time, we may close your PR if it isn't
showing any activity.
