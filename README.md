# Computer Networks — TCP Socket Programming

## Project Overview

A Python implementation of a TCP-based, topic-oriented file distribution service for the IT305 Socket Programming Lab.

The server organizes files by topic, and clients request a topic and download its associated files.

## Objectives

- Implement single-threaded and multi-threaded TCP servers.
- Design a client-server application protocol.
- Implement fault injection and transfer recovery.
- Support checkpoint-based resume after connection failure.
- Extend the system with parallel TCP connections.
- Compare blocking and non-blocking socket implementations.
- Measure throughput, completion time, redundant bytes, and recovery overhead.

## Planned Structure

- `Part1/` — Single-threaded and multi-threaded file distribution.
- `Part2_Case1/` — Restart failed transfers from the beginning.
- `Part2_Case2/` — Checkpoint-based transfer recovery.
- `Part2_Case2_Enhanced/` — Parallel transfers, dynamic scheduling, and ETA.
- `Results/` — Experimental CSV logs and graphs.

## Technology

- Python
- TCP sockets
- Threading and synchronization
- Non-blocking I/O
- CSV-based performance logging
- Matplotlib for result visualization

## Development Status

Repository initialized. Implementation and testing will proceed incrementally, starting with Part I.

## Usage

Exact server and client invocation formats will follow the lab specification. Each case will include runnable `server` and `client` entry points and a Makefile.
