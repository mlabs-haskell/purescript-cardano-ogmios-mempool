# purescript-cardano-ogmios-mempool [![cardano-purescript](https://img.shields.io/badge/cardano--purescript?logo=cardano&logoColor=white&label=cardano-purescript&labelColor=blue&color=blue)](https://github.com/mlabs-haskell/cardano-purescript)


This package provides an interface for interacting with the [Ogmios](https://ogmios.dev/) Local TX Monitor. It enables users to monitor transactions in the mempool using Ogmios' WebSocket API. It is primarily used in [`cardano-transaction-lib`](https://github.com/Plutonomicon/cardano-transaction-lib/).

# Usage

Import the necessary functions to interact with the mempool:

```
import Cardano.Ogmios.Mempool (acquireMempoolSnapshot, mempoolSnapshotHasTx, fetchMempoolTxs)
```

The user of the package is responsible for initializing and maintaining the WebSocket connection. A CTL test case demonstrating how to create a WebSocket and use the Mempool API is available in [Test.Ctl.Testnet.Contract.OgmiosMempool](https://github.com/Plutonomicon/cardano-transaction-lib/blob/develop/test/Testnet/Contract/OgmiosMempool.purs). This test provides an example of how to interact with the mempool via WebSockets.
