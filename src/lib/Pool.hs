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

module Pool
  ( Pool,
    createPool,
    withPool,
    destroyPool,
    withResource,
  )
where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Coerce (coerce)
import Data.Foldable (traverse_)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import Pool.Queue

data Pool a = Pool
  { availableResources :: {-# UNPACK #-} !(TVar [PoolEntry a]),
    awaitingResources :: {-# UNPACK #-} !(TDQueue (TMVar (ResourceNotification a))),
    createdResourceCount :: {-# UNPACK #-} !(TVar Int),
    maxResourceCount :: {-# UNPACK #-} !Int,
    resourceExpiry :: {-# UNPACK #-} !NanoSeconds,
    acquireResource :: IO a,
    releaseResource :: a -> IO (),
    reaperThread :: {-# UNPACK #-} !(Thread ())
  }

data ResourceNotification a
  = ResourceAvailable {-# UNPACK #-} !(PoolEntry a)
  | CreationTicket

data PoolEntry a
  = PoolEntry
      {-# UNPACK #-} !NanoSeconds
      a

entryTime :: PoolEntry a -> NanoSeconds
entryTime (PoolEntry t _) = t

entryValue :: PoolEntry a -> a
entryValue (PoolEntry _ a) = a

createPool ::
  -- | acquire resource
  IO a ->
  -- | release resource
  (a -> IO ()) ->
  -- | max resource count
  Int ->
  -- | resource expiry
  MilliSeconds ->
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

withPool :: IO a -> (a -> IO ()) -> Int -> MilliSeconds -> (Pool a -> IO b) -> IO b
withPool acquire release mx expiry = bracket (createPool acquire release mx expiry) destroyPool

purgePool :: Pool a -> IO ()
purgePool p@Pool {..} = do
  xs <- atomically (readTVar availableResources)
  traverse_ (try @SomeException . destroyResource p . entryValue) xs

destroyPool :: Pool a -> IO ()
destroyPool p@Pool {..} = do
  let Thread tid doneVar = reaperThread
  killThread tid
  atomically (readTMVar doneVar) `onException` do
    killThread tid
    atomically (readTMVar doneVar)
  purgePool p

data Thread a
  = Thread {-# UNPACK #-} !ThreadId {-# UNPACK #-} !(TMVar a)

nanoSecondsToSleepTime :: NanoSeconds -> MicroSeconds
nanoSecondsToSleepTime w =
  -- don't sleep less than 1 second
  max 1000000 (ns2td w + 1)

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
  NanoSeconds ->
  TDQueue (TMVar (ResourceNotification a)) ->
  TVar [PoolEntry a] ->
  TVar Int ->
  (a -> IO ()) ->
  IO (Thread ())
forkReaper expiry waiters resourceVar createdResourceCountVar destroyAction = do
  mtid <- myThreadId
  resVar <- newEmptyTMVarIO
  workerCountVar <- newTVarIO 0

  tid <- mask_ $ forkIOWithUnmask \unmask -> do
    let errHandler e = do
          case fromException @AsyncException e of
            Just ThreadKilled -> do
              waitForWorkers
              atomically (putTMVar resVar ())
            _ -> throwTo mtid e >> throwIO e
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
            _ -> reap >>= action
        reap = do
          ct <- getCurrentTime
          (living, dead) <- atomically do
            (living, dead) <- span (\x -> ct - expiry >= entryTime x) <$> readTVar resourceVar
            writeTVar resourceVar living
            pure (living, dead)
          traverse_ releaseWithWorker (map entryValue dead)
          pure $ case living of
            [] -> expirySleepTime
            _ -> nanoSecondsToSleepTime (entryTime $ last living)
        waitForWorkers = do
          atomically do
            readTVar workerCountVar >>= \case
              0 -> pure ()
              _ -> retry

        releaseWithWorker resource = do
          let workerAction x =
                try @SomeException (unmask (destroyResource' destroyAction waiters resourceVar createdResourceCountVar x)) >>= \case
                  Left e -> case fromException @AsyncException e of
                    -- The reaper might kill us before we have
                    -- finished releasing the resources, in which case
                    -- we do not attempt to release the remaining
                    -- resources.
                    Just ThreadKilled -> throwIO e
                    _ -> pure ()
                  Right _ -> pure ()
          atomically do
            modifyTVar' workerCountVar (+ 1)
          forkWorker workerCountVar (workerAction resource)
    action (nanoSecondsToSleepTime expiry) `catch` (uninterruptibleMask_ . errHandler)
  pure (Thread tid resVar)

forkWorker ::
  -- | Worker count
  TVar Int ->
  -- | the work to do
  IO () ->
  -- | worker thread
  IO ()
forkWorker workerCount action = do
  _tid <- forkIO do
    action
    atomically do
      modifyTVar' workerCount (subtract 1)
  pure ()
{-# INLINE forkWorker #-}

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

newtype NanoSeconds
  = NanoSeconds Word64
  deriving newtype (Num, Eq, Ord, Real, Enum, Integral)

newtype MilliSeconds
  = MilliSeconds Word64
  deriving newtype (Num, Eq, Ord, Real, Enum, Integral)

newtype MicroSeconds
  = MicroSeconds Int
  deriving newtype (Num, Eq, Ord, Real, Enum, Integral)

ms2ns :: MilliSeconds -> NanoSeconds
ms2ns (MilliSeconds x) =
  let ns = x * 1000000
   in case ns < x of
        True -> NanoSeconds maxBound
        False -> NanoSeconds ns

ns2td :: NanoSeconds -> MicroSeconds
ns2td (NanoSeconds w) = case microseconds >= largestIntWord of
  True -> MicroSeconds largestInt
  False -> MicroSeconds (fromIntegral w)
  where
    microseconds = div w 1000
    largestInt = maxBound @Int
    largestIntWord = fromIntegral @Int @Word64 largestInt

nap :: MicroSeconds -> IO ()
nap (MicroSeconds x) = threadDelay x

getCurrentTime :: IO NanoSeconds
getCurrentTime = coerce getMonotonicTimeNSec
