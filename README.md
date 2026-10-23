# purescript-node-streams

Java port of the PureScript `node-streams` package. See the
[port maintenance guide](../javapurs/docs/port-maintenance.md)
for source layout, local dependency selection and JVM validation.

## JVM tests

`./bin/test` delegates to the [common runner](../javapurs/docs/testing.md#port-particulier) as `node-streams`. Java streams are eager, so the port keeps a bounded per-stream event history and replays it to listeners added later; `Test.Main` therefore runs all callback assertions before returning. The suite uses the common synchronous protocol.
`./bin/test --help` is read-only and `--clean` rebuilds the backend first. See the [protocol inventory](../javapurs/docs/port-launchers.md) and [M26](../javapurs/docs/testing.md#validation-m26).

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
