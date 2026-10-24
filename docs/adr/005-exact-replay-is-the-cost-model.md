# ADR 005: Exact replay is the cost model

**Status:** accepted · **Date:** 2026-10-23

## Decision

Exact replay is the cost model.

## Rejected alternatives

Optimizer-style estimation; LLM-estimated cost.

## Why

A lake exposes its files, so the cost of a hypothetical index can be computed exactly from statistics. Zero estimation error is the thesis.
