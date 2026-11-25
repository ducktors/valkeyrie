# Contributing to Valkeyrie

Thank you for your interest in contributing to Valkeyrie! We appreciate your help in making this project better.

## Table of Contents

- [How Can I Contribute?](#how-can-i-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Code Contributions](#code-contributions)
- [Development Workflow](#development-workflow)
  - [Setting Up the Development Environment](#setting-up-the-development-environment)
  - [Building and Testing](#building-and-testing)
  - [Coding Standards](#coding-standards)
- [Pull Request Process](#pull-request-process)
- [Communication](#communication)

## How Can I Contribute?

### Reporting Bugs

If you encounter a bug in Valkeyrie, please submit an issue on our [GitHub Issues](https://github.com/ducktors/valkeyrie/issues) page. When filing a bug report, please include:

- A clear and descriptive title
- A detailed description of the issue
- Steps to reproduce the problem
- Expected behavior and what actually happened
- Version information (Node.js version, Valkeyrie version)
- Any relevant code snippets or error messages

### Suggesting Features

We welcome feature suggestions! To suggest a new feature:

1. Check the [GitHub Issues](https://github.com/ducktors/valkeyrie/issues) to see if the feature has already been suggested
2. If not, create a new issue with the label "enhancement"
3. Clearly describe the feature and the problem it solves
4. Provide examples of how the feature would be used

### Code Contributions

Code contributions are welcome through pull requests. Here's how to contribute code:

1. Fork the repository
2. Create a new branch for your feature or bugfix
3. Make your changes
4. Add or update tests as necessary
5. Ensure all tests pass
6. Submit a pull request

## Development Workflow

### Setting Up the Development Environment

To set up your development environment:

```bash
# Clone your fork of the repository
git clone https://github.com/YOUR_USERNAME/valkeyrie.git
cd valkeyrie

# Install dependencies
pnpm install
```

### Building and Testing

```bash
# Build the project
pnpm build

# Run tests
pnpm test

# Run linting
pnpm lint
```

The tests run on the built-in SQLite driver. To run them on another driver, set `VALKEYRIE_TEST_DRIVER` to a driver name from `test/drivers` or to the path of a module that default-exports a `TestDriver` (see `test/drivers/test-driver.ts`). Tests that only apply to SQLite are skipped on other drivers.

```bash
VALKEYRIE_TEST_DRIVER=./my-driver.ts pnpm test
```

The Redis driver's own tests (`test/redis-driver.ts`) run only when `VALKEYRIE_TEST_REDIS_URL` points to a Redis or Valkey server. To run them, and the shared suite on the Redis driver:

```bash
docker run -d --rm -p 6379:6379 valkey/valkey:8
VALKEYRIE_TEST_REDIS_URL=redis://localhost:6379 VALKEYRIE_TEST_DRIVER=redis pnpm test
```

The MongoDB driver's own tests (`test/mongodb-driver.ts`) run only when `VALKEYRIE_TEST_MONGODB_URL` points to a MongoDB replica set. A single-node replica set is enough:

```bash
docker run -d --rm --name valkeyrie-mongodb -p 27017:27017 mongo:8 --replSet rs0
docker exec valkeyrie-mongodb mongosh --quiet --eval 'rs.initiate()'
VALKEYRIE_TEST_MONGODB_URL='mongodb://localhost:27017/?directConnection=true' VALKEYRIE_TEST_DRIVER=mongodb pnpm test
```

### Coding Standards

- Follow the existing code style
- Write clear, commented code
- Include tests for new features or bug fixes
- Update documentation as needed

## Pull Request Process

1. Update the README.md or documentation with details of changes if appropriate
2. Update the tests to cover your changes
3. Ensure your code passes all tests and linting
4. The PR will be merged once it receives approval from maintainers

## Communication

All project communication, documentation, bug reports, and pull requests should be in English.

For questions about using Valkeyrie, please open a GitHub Discussion rather than an issue.

### Redis Cluster Tests

The cluster fixture starts one primary and one replica. It is separate from the standalone server.

```bash
docker run -d --rm --name valkeyrie-cluster -p 17300:17300 -p 17301:17301 \
  -v "$PWD/test/fixtures/redis-cluster.sh:/cluster.sh:ro" redis:7.2 sh /cluster.sh
```

Wait until `docker exec valkeyrie-cluster test -f /tmp/cluster-ready` succeeds, then run:

```bash
VALKEYRIE_TEST_REDIS_CLUSTER_URL=redis://127.0.0.1:17300 node --test test/redis-cluster.ts
docker stop valkeyrie-cluster
```
