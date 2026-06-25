# Chaintable write node

> Fork of [worldcoin/world-chain](https://github.com/worldcoin/world-chain), with Chaintable pipeline patches.

## Architecture

This repo runs the chain's execution layer with the [Chaintable pipeline](https://github.com/Chaintable/pipeline) tracer embedded. The tracer extracts block data — block headers, transactions, call traces, receipts, events, and state diffs — and ships it to **S3 + Kafka** (see pipeline's [architecture](https://github.com/Chaintable/pipeline/blob/main/docs/architecture.md)). Two consumption paths:

- **Block headers + state diffs** → Kafka + S3 → [leafage-evm](https://github.com/Chaintable/leafage-evm): a lightweight EVM executor serving state queries (`eth_call`, `eth_estimateGas`, …), no P2P sync, no tx storage (see its [architecture](https://github.com/Chaintable/leafage-evm#architecture)).
- **Block files** (transactions · call traces · receipts · events) → S3 → Chaintable's transaction/trace indexing pipeline.

```
Chaintable write node (this repo · producer, embeds pipeline tracer)
        │
        ├─ block headers + state diffs ──────────────────→ Kafka + S3 ─→ leafage-evm (EVM state queries)
        │
        └─ block files (tx · trace · receipts · events) ──→ S3 ─→ Chaintable indexing pipeline (tx/trace data)
```

---


<p align="center">
  <img src="assets/world-chain.png" alt="World Chain">
</p>

# World Chain

World Chain is a blockchain designed for humans. Built on the [OP Stack](https://stack.optimism.io/) and powered by [reth](https://github.com/paradigmxyz/reth), World Chain prioritizes scalability and accessibility for real users, providing the rails for a frictionless onchain UX. 

See the [Development Guide](docs/development.md), and the [Specs](specs/overview.md) for documentation on the protocol.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
