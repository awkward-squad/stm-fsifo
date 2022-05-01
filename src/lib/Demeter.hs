{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Demeter
  ( -- $introduction
    Pool,
    withPool,
    withResource,
    Milliseconds (..),

    -- * Discouraged (but sometimes necessary) alternatives to 'withPool'
    createPool,
    destroyPool,
  )
where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Coerce (coerce)
import Data.Foldable (traverse_)
import Data.Word (Word64)
import Demeter.Queue
import GHC.Clock (getMonotonicTimeNSec)

data Pool a = Pool
  { availableResources :: {-# UNPACK #-} !(TVar [PoolEntry a]),
    awaitingResources :: {-# UNPACK #-} !(TDQueue (TMVar (ResourceNotification a))),
    createdResourceCount :: {-# UNPACK #-} !(TVar Int),
    maxResourceCount :: {-# UNPACK #-} !Int,
    resourceExpiry :: {-# UNPACK #-} !Nanoseconds,
    acquireResource :: IO a,
    releaseResource :: a -> IO (),
    reaperThread :: {-# UNPACK #-} !(Thread ())
  }

data ResourceNotification a
  = ResourceAvailable {-# UNPACK #-} !(PoolEntry a)
  | CreationTicket

data PoolEntry a
  = PoolEntry
      {-# UNPACK #-} !Nanoseconds
      a

entryTime :: PoolEntry a -> Nanoseconds
entryTime (PoolEntry t _) = t

entryValue :: PoolEntry a -> a
entryValue (PoolEntry _ a) = a

-- | See 'withPool'
createPool ::
  IO a ->
  (a -> IO ()) ->
  Int ->
  Milliseconds ->
  IO (Pool a)
createPool acquire release mx expiry = do
  let expiryNs = ms2ns expiry
  resourceVar <- newTVarIO []
  waiterVar <- newTDQueueIO
  resourceCount <- newTVarIO 0
  reaperThread <- forkReaper expiryNs waiterVar resourceVar resourceCount release
  let pool =
        Pool
          { availableResources = resourceVar,
            awaitingResources = waiterVar,
            createdResourceCount = resourceCount,
            maxResourceCount = mx,
            resourceExpiry = expiryNs,
            acquireResource = acquire,
            releaseResource = release,
            reaperThread = reaperThread
          }
  pure pool

-- | Execute an action with a new resource pool.
--
-- When the callback returns the 'Pool' will be destroyed and all
-- acquired resources will be released.
withPool ::
  -- | How to acquire a resource
  IO a ->
  -- | How to release a resource
  (a -> IO ()) ->
  -- | The largest number of resources that might be acquired but not
  -- yet released
  Int ->
  -- | The amount of time that an acquired resource can go unused
  -- before it is released
  Milliseconds ->
  -- | The action to execute with a new @Pool@
  (Pool a -> IO b) ->
  IO b
withPool acquire release mx expiry = bracket (createPool acquire release mx expiry) destroyPool

-- | See 'withPool'
destroyPool :: Pool a -> IO ()
destroyPool Pool {..} = do
  let Thread tid doneVar = reaperThread
  uninterruptibleMask_ do
    killThread tid
    atomically (readTMVar doneVar)

data Thread a
  = Thread {-# UNPACK #-} !ThreadId {-# UNPACK #-} !(TMVar a)

nanoSecondsToSleepTime :: Nanoseconds -> Microseconds
nanoSecondsToSleepTime w =
  -- don't sleep less than 1 second
  max 1000000 (ns2td w + 1)

data UnexpectedReaperException
  = UnexpectedReaperException SomeException
  deriving stock (Show)

instance Exception UnexpectedReaperException where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- | The reaper is responsible for releasing expired
-- resources. Because we don't know how long it might take to release
-- the resources, and we don't want a long-running release to delay
-- the release of other expired resources, the reaper does not release
-- any resources itself, but sends each batch of releases to a child
-- thread for releasing. These children threads are kept track of in a
-- 'TDQueue'.
--
-- When the reaper is killed it waits for the children to finish
-- releasing the resources it had already checked out, but if a second
-- exception is received then it kills all children and signals completion.
forkReaper ::
  Nanoseconds ->
  TDQueue (TMVar (ResourceNotification a)) ->
  TVar [PoolEntry a] ->
  TVar Int ->
  (a -> IO ()) ->
  IO (Thread ())
forkReaper expiry waiters resourceVar createdResourceCountVar destroyAction = do
  mtid <- myThreadId
  resVar <- newEmptyTMVarIO
  workerCountVar <- newTVarIO @Int 0

  tid <- mask_ $ forkIOWithUnmask \unmask -> do
    let errHandler e = do
          case fromException @AsyncException e of
            Just ThreadKilled -> do
              reap =<< readTVarIO resourceVar
              waitForWorkers
            _ -> do
              throwTo mtid (UnexpectedReaperException e)
          atomically (putTMVar resVar ())
        expirySleepTime = nanoSecondsToSleepTime expiry
        action sleepTime = do
          nap sleepTime
          readTVarIO resourceVar >>= \case
            -- If the resource var is empty, then we add ourselves as
            -- a waiter to the resourceVar and return to sleep. Once a
            -- value is put to the resource var we restart our loop
            -- with 'expiry' sleep time (as a freshly returned
            -- resource will remain valid for at least 'expiry' time).
            --
            -- This avoids waking up the reaper if there is no pool
            -- activity or so much activity that resources are
            -- returned straight to waiters.
            [] -> atomically (guard . not . null =<< readTVar resourceVar) >> action (nanoSecondsToSleepTime expiry)
            _ -> reapDead >>= action
        reapDead = do
          ct <- getCurrentTime
          (living, dead) <- atomically do
            (living, dead) <- span (\x -> ct - expiry >= entryTime x) <$> readTVar resourceVar
            writeTVar resourceVar living
            pure (living, dead)
          reap dead
          pure $ case living of
            [] -> expirySleepTime
            _ -> nanoSecondsToSleepTime (entryTime $ last living)
        reap xs = do
          traverse_ (releaseWithWorker . entryValue) xs
        waitForWorkers = do
          atomically do
            readTVar workerCountVar >>= \case
              0 -> pure ()
              _ -> retry

        releaseWithWorker resource = do
          atomically do
            modifyTVar' workerCountVar (+ 1)
          _tid <- forkIO do
            unmask (destroyResource' destroyAction waiters resourceVar createdResourceCountVar resource) `catch` \(_ :: SomeException) -> pure ()
            atomically do
              modifyTVar' workerCountVar (subtract 1)
          pure ()
    action (nanoSecondsToSleepTime expiry) `catch` (uninterruptibleMask_ . errHandler)
  pure (Thread tid resVar)

returnResourceSTM :: Pool a -> ResourceNotification a -> STM ()
returnResourceSTM Pool {..} c = returnResourceSTM' awaitingResources availableResources createdResourceCount c
{-# INLINE returnResourceSTM #-}

returnResourceSTM' ::
  TDQueue (TMVar (ResourceNotification a)) ->
  TVar [PoolEntry a] ->
  TVar Int ->
  ResourceNotification a ->
  STM ()
returnResourceSTM' awaitingResources availableResources createdResourceCount c = do
  pop awaitingResources >>= \case
    Nothing -> case c of
      ResourceAvailable v -> modifyTVar availableResources (v :)
      CreationTicket -> modifyTVar' createdResourceCount (\x -> x - 1)
    Just w -> putTMVar w c
{-# INLINE returnResourceSTM' #-}

popAvailable :: Pool a -> STM (Maybe (PoolEntry a))
popAvailable Pool {..} = do
  readTVar availableResources >>= \case
    [] -> pure Nothing
    x : xs -> do
      writeTVar availableResources xs
      pure (Just x)
{-# INLINE popAvailable #-}

returnResource :: Pool a -> a -> IO ()
returnResource a b = do
  ct <- getCurrentTime
  atomically (returnResourceSTM a (ResourceAvailable (PoolEntry ct b)))
{-# INLINE returnResource #-}

destroyResource :: Pool a -> a -> IO ()
destroyResource Pool {..} c = do
  destroyResource' releaseResource awaitingResources availableResources createdResourceCount c
{-# INLINE destroyResource #-}

destroyResource' ::
  (a -> IO ()) ->
  TDQueue (TMVar (ResourceNotification a)) ->
  TVar [PoolEntry a] ->
  TVar Int ->
  a ->
  IO ()
destroyResource' release waiters rs crc r = do
  release r `onException` atomically (returnResourceSTM' waiters rs crc CreationTicket)
  atomically (returnResourceSTM' waiters rs crc CreationTicket)
{-# INLINE destroyResource' #-}

withResource :: Pool a -> (a -> IO b) -> IO b
withResource p@(Pool {..}) k = do
  let getConn = (join . atomically) do
        popAvailable p >>= \case
          Just (PoolEntry _ x) -> pure (pure x)
          Nothing -> do
            resourceCount <- readTVar createdResourceCount
            case resourceCount < maxResourceCount of
              True -> do
                writeTVar createdResourceCount $! resourceCount + 1
                pure (acquireResource `onException` atomically (returnResourceSTM p CreationTicket))
              False -> do
                var <- newEmptyTMVar
                removeSelf <- push awaitingResources var
                let handleNotif = \case
                      ResourceAvailable x -> pure (entryValue x)
                      CreationTicket -> acquireResource `onException` atomically (returnResourceSTM p CreationTicket)
                    dequeue = atomically do
                      removeSelf
                      tryTakeTMVar var >>= \case
                        Nothing -> pure ()
                        Just v -> returnResourceSTM p v
                pure (handleNotif =<< (atomically (takeTMVar var) `onException` dequeue))

  mask \restore -> do
    c <- getConn
    r <- restore (k c) `onException` destroyResource p c
    returnResource p c
    pure r
{-# INLINEABLE withResource #-}

newtype Nanoseconds
  = Nanoseconds Word64
  deriving newtype (Num, Eq, Ord, Real, Enum, Integral)

newtype Milliseconds
  = Milliseconds Word64
  deriving newtype (Eq, Ord, Enum)

newtype Microseconds
  = Microseconds Int
  deriving newtype (Num, Eq, Ord, Real, Enum, Integral)

ms2ns :: Milliseconds -> Nanoseconds
ms2ns (Milliseconds x) =
  let ns = x * 1000000
   in case ns < x of
        True -> Nanoseconds maxBound
        False -> Nanoseconds ns

ns2td :: Nanoseconds -> Microseconds
ns2td (Nanoseconds w) = case microseconds >= largestIntWord of
  True -> Microseconds largestInt
  False -> Microseconds (fromIntegral w)
  where
    microseconds = div w 1000
    largestInt = maxBound @Int
    largestIntWord = fromIntegral @Int @Word64 largestInt

nap :: Microseconds -> IO ()
nap (Microseconds x) = threadDelay x

getCurrentTime :: IO Nanoseconds
getCurrentTime = coerce getMonotonicTimeNSec

-- $introduction
-- A resource pool that uses minimal cpu under high contention.
--
-- There are two parameters to configure:
--
-- 1. The largest number of resources that might be acquired but not
-- yet released
--
-- 2. The amount of time that an acquired resource can go unused
-- before it is released
