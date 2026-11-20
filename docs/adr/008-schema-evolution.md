# Explicit JSON contracts

Status: accepted for 0.1.0-dev.

## Context

A small domain needs visible, testable v1/v2 evolution without introducing registry infrastructure prematurely.

## Decision

Use JSON with explicit type/version dispatch; order.created v2 adds optional channel with unknown default. Current consumers support v1/v2. Ignore Java type headers, reject unsupported versions.

## Consequences

This provides tested backward compatibility, not universal forward compatibility or full schema governance. Future incompatible contracts need fixtures and rollout/replay planning.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
