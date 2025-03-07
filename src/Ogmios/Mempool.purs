-- | A module for interacting with Ogmios' Local TX Monitor
-- | These functions only work with Ogmios backend (not Blockfrost!).
-- | https://ogmios.dev/mini-protocols/local-tx-monitor/
module Cardano.Ogmios.Mempool
  ( acquireMempoolSnapshot
  , mempoolSnapshotHasTx
  , mempoolSnapshotNextTx
  , fetchMempoolTxs
  , mempoolSnapshotSizeAndCapacity
  , releaseMempool
  , withMempoolSnapshot
  , MempoolEnv
  , MempoolMT(MempoolMT)
  , MempoolM
  , module X
  ) where

import Prelude

import Cardano.AsCbor (decodeCbor)
import Cardano.Kupmios.Logging (Logger, mkLogger)
import Cardano.Ogmios.Internal.Mempool
  ( ReleasedMempool(ReleasedMempool)
  , MempoolSizeAndCapacity(MempoolSizeAndCapacity)
  , MempoolSnapshotAcquired
  , MempoolTransaction(MempoolTransaction)
  , HasTxR(HasTxR)
  , MaybeMempoolTransaction(MaybeMempoolTransaction)
  , acquireMempoolSnapshotCall
  , mempoolSnapshotHasTxCall
  , mempoolSnapshotNextTxCall
  , mempoolSnapshotSizeAndCapacityCall
  , releaseMempoolCall
  , ListenerSet
  , OgmiosListeners
  , ListenerId
  , mkOgmiosCallType
  , OgmiosWebSocket
  , WebSocket(WebSocket)
  , listeners
  , mkListenerSet
  , defaultMessageListener
  , mkOgmiosWebSocketAff
  , mkRequestAff
  , underlyingWebSocket
  ) as X
import Cardano.Ogmios.Internal.Mempool
  ( MempoolSizeAndCapacity
  , MempoolSnapshotAcquired
  , MempoolTransaction(MempoolTransaction)
  ) as Ogmios
import Cardano.Ogmios.Internal.Mempool.JsWebSocket (JsWebSocket)
import Cardano.Ogmios.Internal.Mempool.JsonRpc2 as JsonRpc2
import Cardano.Types.Transaction (Transaction)
import Cardano.Types.TransactionHash (TransactionHash)
import Control.Monad.Error.Class
  ( class MonadError
  , class MonadThrow
  , liftMaybe
  , throwError
  , try
  )
import Control.Monad.Reader.Class (class MonadAsk)
import Control.Monad.Reader.Trans (ReaderT(ReaderT), asks)
import Data.Array as Array
import Data.ByteArray (hexToByteArray)
import Data.Either (either)
import Data.List (List(Cons))
import Data.Log.Level (LogLevel)
import Data.Log.Message (Message)
import Data.Maybe (Maybe(Just, Nothing))
import Data.Newtype (class Newtype, unwrap, wrap)
import Data.Traversable (for)
import Effect.Aff (Aff)
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Class (class MonadEffect)
import Effect.Exception (Error, error)

----------------
-- Mempool monad
----------------

type MempoolEnv =
  { ogmiosWs :: X.OgmiosWebSocket
  , logLevel :: LogLevel
  , customLogger :: Maybe (LogLevel -> Message -> Aff Unit)
  , suppressLogs :: Boolean
  }

type MempoolM = MempoolMT Aff

newtype MempoolMT (m :: Type -> Type) (a :: Type) =
  MempoolMT (ReaderT MempoolEnv m a)

derive instance Newtype (MempoolMT m a) _
derive newtype instance Functor m => Functor (MempoolMT m)
derive newtype instance Apply m => Apply (MempoolMT m)
derive newtype instance Applicative m => Applicative (MempoolMT m)
derive newtype instance Bind m => Bind (MempoolMT m)
derive newtype instance Monad (MempoolMT Aff)
derive newtype instance MonadEffect (MempoolMT Aff)
derive newtype instance MonadAff (MempoolMT Aff)
derive newtype instance MonadThrow Error (MempoolMT Aff)
derive newtype instance MonadError Error (MempoolMT Aff)
derive newtype instance MonadAsk MempoolEnv (MempoolMT Aff)

--------------------
-- Mempool functions
--------------------

-- | A bracket-style function for working with mempool snapshots - ensures
-- | release in the presence of exceptions
withMempoolSnapshot
  :: forall a
   . (Ogmios.MempoolSnapshotAcquired -> MempoolM a)
  -> MempoolM a
withMempoolSnapshot f = do
  s <- acquireMempoolSnapshot
  res <- try $ f s
  releaseMempool s
  either throwError pure res

-- | Recursively request the next TX in the mempool until Ogmios does not
-- | respond with a new TX.
fetchMempoolTxs
  :: Ogmios.MempoolSnapshotAcquired
  -> MempoolM (Array Transaction)
fetchMempoolTxs ms = Array.fromFoldable <$> go
  where
  go = do
    nextTX <- mempoolSnapshotNextTx ms
    case nextTX of
      Just tx -> Cons tx <$> go
      Nothing -> pure mempty

acquireMempoolSnapshot
  :: MempoolM Ogmios.MempoolSnapshotAcquired
acquireMempoolSnapshot =
  mkOgmiosRequest
    X.acquireMempoolSnapshotCall
    _.acquireMempool
    unit

mempoolSnapshotHasTx
  :: Ogmios.MempoolSnapshotAcquired
  -> TransactionHash
  -> MempoolM Boolean
mempoolSnapshotHasTx ms txh =
  unwrap <$> mkOgmiosRequest
    (X.mempoolSnapshotHasTxCall ms)
    _.mempoolHasTx
    txh

mempoolSnapshotSizeAndCapacity
  :: Ogmios.MempoolSnapshotAcquired
  -> MempoolM Ogmios.MempoolSizeAndCapacity
mempoolSnapshotSizeAndCapacity ms =
  mkOgmiosRequest
    (X.mempoolSnapshotSizeAndCapacityCall ms)
    _.mempoolSizeAndCapacity
    unit

releaseMempool
  :: Ogmios.MempoolSnapshotAcquired
  -> MempoolM Unit
releaseMempool ms =
  unit <$ mkOgmiosRequest
    (X.releaseMempoolCall ms)
    _.releaseMempool
    unit

mempoolSnapshotNextTx
  :: Ogmios.MempoolSnapshotAcquired
  -> MempoolM (Maybe Transaction)
mempoolSnapshotNextTx ms = do
  mbTx <- unwrap <$> mkOgmiosRequest
    (X.mempoolSnapshotNextTxCall ms)
    _.mempoolNextTx
    unit
  for mbTx \(Ogmios.MempoolTransaction { raw }) -> do
    byteArray <- liftMaybe (error "Failed to decode transaction")
      $ hexToByteArray raw
    liftMaybe (error "Failed to decode tx")
      $ decodeCbor
      $ wrap byteArray

-- | Builds an Ogmios request action using `MempoolM`
mkOgmiosRequest
  :: forall (request :: Type) (response :: Type)
   . JsonRpc2.JsonRpc2Call request response
  -> (X.OgmiosListeners -> X.ListenerSet request response)
  -> request
  -> MempoolM response
mkOgmiosRequest jsonRpc2Call getLs inp = do
  listeners' <- asks $ X.listeners <<< _.ogmiosWs
  websocket <- asks $ X.underlyingWebSocket <<< _.ogmiosWs
  mkRequest listeners' websocket jsonRpc2Call getLs inp

mkRequest
  :: forall (request :: Type) (response :: Type) (listeners :: Type)
   . listeners
  -> JsWebSocket
  -> JsonRpc2.JsonRpc2Call request response
  -> (listeners -> X.ListenerSet request response)
  -> request
  -> MempoolM response
mkRequest listeners' ws jsonRpc2Call getLs inp = do
  logger <- getLogger
  liftAff $ X.mkRequestAff listeners' ws logger jsonRpc2Call getLs inp
  where
  getLogger :: MempoolM Logger
  getLogger = do
    logLevel <- asks $ _.logLevel
    mbCustomLogger <- asks $ _.customLogger
    pure $ mkLogger logLevel mbCustomLogger
