# purescript-node-streams

## JVM tests

`./bin/test` delegates to the [common runner](../javapurs/docs/testing.md#port-particulier) as `node-streams`, but currently exits **1** with an unsupported-completion diagnostic: `Test.Main` leaves assertions in stream callbacks without a completion action.
`./bin/test --help` is read-only. Even with `--clean`, this unsupported protocol is rejected before build/workspace creation; the checkout and its outputs are preserved. See the [protocol inventory](../javapurs/docs/port-launchers.md).

[![Latest release](http://img.shields.io/github/release/purescript-node/purescript-node-streams.svg)](https://github.com/purescript-node/purescript-node-streams/releases)
[![Build status](https://github.com/purescript-node/purescript-node-streams/workflows/CI/badge.svg?branch=master)](https://github.com/purescript-node/purescript-node-streams/actions?query=workflow%3ACI+branch%3Amaster)
[![Pursuit](https://pursuit.purescript.org/packages/purescript-node-streams/badge)](https://pursuit.purescript.org/packages/purescript-node-streams)

A wrapper for Node's [Stream API](https://nodejs.org/api/stream.html).

See the `example` directory for a usage example.

## Installation

```
spago install node-streams
```

## Documentation

Module documentation is [published on Pursuit](http://pursuit.purescript.org/packages/purescript-node-streams).
