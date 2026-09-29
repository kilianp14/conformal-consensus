# Conformal Consensus

A Rust prototype exploring adaptive fast-path consensus using conformal risk control.

## About

Conformal Consensus extends a trimmed-down [OmniPaxos](https://github.com/haraldng/omnipaxos) implementation with a hybrid execution strategy: nodes independently choose between a conservative and a fast execution path based on locally observed network conditions. A lightweight heuristic estimates fast-path success, while online conformal risk control adapts the decision threshold to target a user-defined fast-path failure rate.

The implementation also includes an in-memory replicated key-value store, distributed benchmarking infrastructure, Docker configurations, and scripts for running experiments on Google Cloud Platform. Experiments were conducted on five-node clusters in Europe and the US under controlled, bursty workloads. See [`push_images.sh`](push_images.sh), [`create_nodes.sh`](create_nodes.sh), and [`run_experiments.sh`](run_experiments.sh) for the full GCP-based evaluation workflow.

This is a research prototype rather than a production-ready consensus implementation.

## Thesis

This repository contains the software artifacts and experiment infrastructure for my master's thesis:

**[Optimistic Consensus under Conformal Risk Control](https://github.com/kilianp14/master-thesis-kth)**

The thesis provides the protocol design, methodology, detailed results, and discussion of limitations.

## Attribution

This project builds on the open-source [OmniPaxos](https://github.com/haraldng/omnipaxos) implementation. See [`NOTICE`](NOTICE) for attribution details.
