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

# Run benchmarks
pnpm benchmark
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
