{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UnboxedTuples #-}

-- | A resource pool manages the lifecycle and distribution of
-- resources across multiple threads, where a "resource" is anything
-- that must be released once acquired. A PostgreSQL connection is an
-- example of a resource.
--
-- == Demeter balances the following constraints:
--
-- === It is desirable to minimize the number of acquire or release calls
--
-- For example, acquiring a PostgreSQL connection requires
-- establishing a network connection to the PostgreSQL server and
-- going through an authorization handshake.
--
-- It might also be the case that acquiring a resource is cheap but
-- releasing a resource is expensive &#8212; for example, SQLite
-- connections contain a page cache that becomes more and more costly
-- to rebuild if thrown away.
--
-- === It is desirable to limit the number of live resources
--
-- For example, the PostgreSQL server has a (configurable) maximum
-- number of concurrent connections (some shared memory is allocated
-- when the server starts for each potential connection).
--
-- === It is undesirable to keep more live resources than necessary
--
-- For example, PostgreSQL server performance degrades as the number
-- of open connections increases.
--
-- == __How Demeter works:__
--
-- To alleviate the cost of acquiring and releasing resources, it
-- would be good to reuse them. However, it is undesirable to keep
-- acquired resources around for reuse if they aren't necessary.
--
-- To balance these conflicting concerns, after a resource is acquired
-- and returned to the pool it is not released immediately &#8212; it
-- is instead stored in a LIFO queue of /available resources/ so that
-- it can be provided to future resource requests. If the resource
-- goes unused for some configurable amount of time, it will be
-- automatically released.
--
-- The available resource queue is LIFO (rather than FIFO) so that a
-- traffic burst followed by consistent but infrequent requests does
-- not keep alive more resources than necessary. Suppose, for example,
-- that there is a resource limit of 10 and an expiration time of 10
-- seconds. A burst of requests happens that causes all 10 resources
-- to be acquired, then there is a lighter workload of one resource
-- request per 500ms, where each request returns the resource 100ms
-- after taking it. This "lighter workload" period can clearly be
-- handled by a single connection, but a FIFO queue would result in
-- each resource getting used before expiring. On the other hand, a
-- LIFO queue ensures that the same resource is used for each request,
-- allowing the 9 unused resources to reach their expiration time and
-- be released.
--
-- The flow for acquiring a resource is:
--
--   * If a resource is available then use it, otherwise:
--
--       * If the resource limit has not been reached then acquire a new resource and use it, otherwise:
--
--           * Block and wait for a resource to be returned to the pool or destroyed
--
-- @STM@ is used, for both ease of enforcing serializability /and for performance/.
-- If the available resources were protected by an
-- @MVar@ then threads would be removed from the run queue when it
-- would be preferable that they simply try again immediately. For
-- example, suppose there are two concurrent threads returning
-- resources to the pool, @T1@ and @T2@. @T1@ takes the @MVar@ lock in
-- order to push onto the LIFO, then @T2@ blocks on taking the @MVar@
-- and goes to sleep (/i.e./ adds itself to the @MVar@'s queue and
-- removes itself from the run queue). When the @T1@ puts it will
-- attempt to wake up @T2@ (/i.e./ send a message to @T2@'s capability
-- that @T2@ can be pushed onto the back of its run queue).
--
-- This is a (relatively) large delay that could result in a number of
-- resource requests blocking because resources aren't being returned
-- as promptly as they ought to be. A similar situation arises when
-- @N@ threads concurrently take from an available resources LIFO that
-- contains at least @N@ resources. None of the threads benefit from
-- blocking in this scenario. It would be preferable for them to retry
-- immediately.
--
-- @STM@ achieves the desired behavior. Indeed, if @T1@ committed its
-- transaction first then @T2@ will fail its validation step and
-- /immediately retry the transaction/, avoiding the expensive process
-- of going to sleep and being woken up. A thread will only sleep if
-- there are no available resources, the resource limit is reached,
-- and the @STM@ validation step succeeds (/i.e./ No thread mutated
-- these variables since we read them).
--
-- @STM@ comes with a significant drawbacks though:
--
--   1. When the "available resources" or "number of acquired
--      resources" variables are mutated then all blocked threads are
--      signaled that they should be awoken and retry their
--      transaction. However, the mutation that signals them must be a
--      single resource being returned or marked as destroyed, so only
--      one of these awoken threads will actually get the resource and
--      the rest are just wasting cpu cycles.
--
--   2. We lose fair resource distribution: There is no queue of
--      threads waiting on a resource &#8212; all threads attempt to
--      grab the resource when signaled that there is a chance they
--      could get it. Thus, a thread can be delayed from acquiring a
--      resource on a busy pool for an arbitrarily long time.
--
-- It is desirable then to amend the flow so that at most one thread
-- is awoken when a resource is returned or destroyed, and a thread
-- does not acquire a resource before prior waiting threads obtain a
-- resource. To this end, a FIFO queue of /waiters/ is included in
-- the pool as well. When taking a resource, if the available
-- resources queue is empty and the resource limit has been reached
-- then the thread adds a @TMVar@ to the FIFO queue and blocks on
-- taking from this @TMVar@. When a resource is returned to the pool
-- then the flow is:
--
--   * If the waiter queue is non-empty then return the resource straight to the first waiter, otherwise
--
--       * Return the resource to the available resources queue
--
-- If a resource is destroyed then the flow is
--
--  * If the waiter queue is non-empty then signal the first waiter that it may acquire a resource, otherwise
--
--      * Decrement the resource count variable
--
-- The only remaining trouble is the threads waiting on a resource
-- might be thrown an async exception. Thus, the waiter queue ought to
-- support /O(1)/ delete so that a thread can remove itself from the
-- queue upon receiving an exception. A queue with this property is
-- provided by the @stm-fsifo@ package.
--
-- With these changes @demeter@ achieves the throughput gains
-- associated with STM retrying while mitigating the corresponding
-- drawbacks of retrying transactions too often and unfair resource
-- distribution.
module Demeter
  ( -- * Pool management
    Pool,
    withPool,

    -- ** Unsafe primitives

    -- | Helpful if the desired usage is incompatible with 'withPool'
    -- (/e.g./ using
    -- [Acquire](https://hackage.haskell.org/package/acquire))
    createPool,
    destroyPool,

    -- * Resource management
    withResource,

    -- ** Unsafe primitives
    takeResource,
    returnResource,

    -- * Types
    Milliseconds (..),
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent hiding (forkIO)
import Control.Concurrent.STM
import Control.Concurrent.STM.Fsifo
import Control.Exception
import Control.Monad
import Data.Coerce (coerce)
import Data.Foldable (traverse_)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Conc (ThreadId (ThreadId))
import GHC.Exts (fork#)
import GHC.IO (IO (IO), unsafeUnmask)

-- | A resource pool
data Pool a = Pool
  { -- The resources that have been used and returned to the pool. If they remain here for `resourceExpiry`, then
    -- they'll be released by the reaper thread. The only way a resource can end up here is if there are no waiters in
    -- `awaitingResources` at the time it's returned to the pool. The resources are approximately in descending return
    -- time order: the head of the list contains the resource that was returned most recently, with the greatest (or
    -- very close to the greatest) `entryTime`.
    availableResources :: {-# UNPACK #-} !(TVar [PoolEntry a]),
    -- A queue of waiters. When we go to return a resource to the pool, we'll first try handing it to the first waiter
    -- in the queue, if any.
    awaitingResources :: {-# UNPACK #-} !(Fsifo (TMVar (ResourceNotification a))),
    -- The total number of resources we've either created, or have committed to creating. All resources we've committed
    -- to creating will be in the form of `CreationTicket`s in the `availableResources` list, which allow takers to
    -- (attempt to) create a resource without bumping this counter again.
    createdResourceCount :: {-# UNPACK #-} !(TVar Int),
    -- The maximum number of resources that can be created at any given time.
    maxResourceCount :: {-# UNPACK #-} !Int,
    -- The amount of time a resource can sit in `availableResources` before it is released.
    resourceExpiry :: {-# UNPACK #-} !Nanoseconds,
    acquireResource :: IO a,
    releaseResource :: a -> IO (),
    reaperThread :: {-# UNPACK #-} !(Thread (Maybe UnexpectedReaperException))
  }

-- A "resource notification" is what one waiting on a resource will receive: either a resource, or permission to create
-- a new resource.
data ResourceNotification a
  = ResourceAvailable {-# UNPACK #-} !(PoolEntry a)
  | -- Permission to create a new resource
    CreationTicket

data PoolEntry a
  = PoolEntry
      -- The monotonic time that this resource was returned to the pool.
      {-# UNPACK #-} !Nanoseconds
      a

entryTime :: PoolEntry a -> Nanoseconds
entryTime (PoolEntry t _) = t

entryValue :: PoolEntry a -> a
entryValue (PoolEntry _ a) = a

createPool ::
  IO a ->
  (a -> IO ()) ->
  Int ->
  Milliseconds ->
  IO (Pool a)
createPool acquire release mx expiry = do
  let expiryNs = ms2ns expiry
  resourceVar <- newTVarIO []
  waiterVar <- newFsifoIO
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

-- | Execute an action with a resource pool.
--
-- When the callback returns the 'Pool' will be destroyed and all
-- acquired resources will be released.
withPool ::
  -- | How to acquire a resource
  IO a ->
  -- | How to release a resource
  (a -> IO ()) ->
  -- | The live resource limit (/i.e./ acquired but not yet released)
  Int ->
  -- | The amount of time that a resource can remain in the pool
  -- before it is released
  Milliseconds ->
  -- | The action to execute with a new @Pool@
  (Pool a -> IO b) ->
  IO b
withPool acquire release mx expiry = bracket (createPool acquire release mx expiry) destroyPool

destroyPool :: Pool a -> IO ()
destroyPool Pool {reaperThread} = do
  let Thread tid doneVar = reaperThread
  uninterruptibleMask_ do
    killThread tid
    atomically (readTMVar doneVar) >>= \case
      Nothing -> pure ()
      Just exception -> throwIO exception

data Thread a
  = Thread {-# UNPACK #-} !ThreadId {-# UNPACK #-} !(TMVar a)

nanoSecondsToSleepTime :: Nanoseconds -> Microseconds
nanoSecondsToSleepTime w =
  -- don't sleep less than 1 second
  max 1_000_000 (ns2td w + 1)

data WorkerDiedByAsyncException
  = WorkerDiedByAsyncException SomeException -- invariant: this is an async exception
  deriving stock (Show)
  deriving anyclass (Exception)

newtype UnexpectedReaperException
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
-- 'Fsifo'.
--
-- When the reaper is killed it waits for the children to finish
-- releasing the resources it had already checked out, but if a second
-- exception is received then it kills all children and signals completion.
forkReaper ::
  Nanoseconds ->
  Fsifo (TMVar (ResourceNotification a)) ->
  TVar [PoolEntry a] ->
  TVar Int ->
  (a -> IO ()) ->
  IO (Thread (Maybe UnexpectedReaperException))
forkReaper expiry waiters resourceVar createdResourceCountVar destroyAction = do
  mtid <- myThreadId
  resVar <- newEmptyTMVarIO
  workerCountVar <- newTVarIO @Int 0
  -- If an async exception is raised in a worker, the worker (tries to) place the exception here
  workerExceptionVar <- newEmptyTMVarIO

  tid <- mask_ $ forkIOWithUnmask \unmask -> do
    let errHandler e = uninterruptibleMask_ do
          reap =<< readTVarIO resourceVar
          -- FIXME we don't want to wait for workers, then have a new worker spawned after
          waitForWorkers
          let propagateException e2 =
                unsafeUnmask (try (throwTo mtid e2)) >>= \case
                  Left e3
                    | Just ThreadKilled <- fromException e3 -> atomically (putTMVar resVar (Just e2))
                    | otherwise -> propagateException e2
                  Right () -> atomically (putTMVar resVar Nothing)
          case () of
            ()
              | Just ThreadKilled <- fromException e ->
                  atomically do
                    maybeWorkerException <- tryTakeTMVar workerExceptionVar
                    putTMVar resVar $
                      coerce @(Maybe SomeException) @(Maybe UnexpectedReaperException) maybeWorkerException
              | Just (WorkerDiedByAsyncException e2) <- fromException e ->
                  propagateException (UnexpectedReaperException e2)
              | otherwise -> propagateException (UnexpectedReaperException e)
        expirySleepTime = nanoSecondsToSleepTime expiry
        action sleepTime = do
          signal <- registerDelay (coerce @Microseconds @Int sleepTime)
          (join . unmask . atomically) do
            let workerDiedHandler = do
                  exception <- readTMVar workerExceptionVar
                  pure (errHandler (toException (WorkerDiedByAsyncException exception)))
            let naptimeOverHandler = do
                  readTVar signal >>= \case
                    False -> retry
                    True -> pure ()
                  pure do
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
                      [] -> atomically (guard . not . null =<< readTVar resourceVar) >> action expirySleepTime
                      _ -> reapDead >>= action
            workerDiedHandler <|> naptimeOverHandler
        reapDead = do
          ct <- getCurrentTime
          let expiryAgo = ct - expiry
          (living, dead) <- atomically do
            (living, dead) <- span (\x -> expiryAgo < entryTime x) <$> readTVar resourceVar
            writeTVar resourceVar living
            pure (living, dead)
          reap dead
          pure case living of
            [] -> expirySleepTime
            _ -> nanoSecondsToSleepTime ((entryTime $ last living) - expiryAgo) -- last living expires first
        reap = traverse_ (releaseWithWorker . entryValue)
        waitForWorkers = do
          atomically do
            readTVar workerCountVar >>= \case
              0 -> pure ()
              _ -> retry

        releaseWithWorker resource = do
          atomically (modifyTVar' workerCountVar (+ 1))
          _tid <- forkIO do
            destroyResource' destroyAction waiters resourceVar createdResourceCountVar resource
              -- We know `e` is an async exception, because destroyResource' silences synchronous exceptions. We don't
              -- want to ignore it, so we propagate it to the reaper, which will eventually propagate it to the thread
              -- that created it.
              `catch` \(e :: SomeException) -> void (atomically (tryPutTMVar workerExceptionVar e))
            atomically (modifyTVar' workerCountVar (subtract 1))
          pure ()
    action expirySleepTime `catch` errHandler
  pure (Thread tid resVar)

-- Return a resource notification to the pool.
--
-- If there is a waiter, hand it to them directly.
--
-- Otherwise,
--   If the resource notification is a real resource, add it to the list of available resources.
--   Otherwise, it's a creation ticket; "return" it by decrementing the created resources count.
returnResourceSTM :: Pool a -> ResourceNotification a -> STM ()
returnResourceSTM Pool {awaitingResources, availableResources, createdResourceCount} =
  returnResourceSTM' awaitingResources availableResources createdResourceCount
{-# INLINE returnResourceSTM #-}

returnResourceSTM' ::
  Fsifo (TMVar (ResourceNotification a)) ->
  TVar [PoolEntry a] ->
  TVar Int ->
  ResourceNotification a ->
  STM ()
returnResourceSTM' awaitingResources availableResources createdResourceCount c =
  popFsifo awaitingResources >>= \case
    Nothing -> case c of
      ResourceAvailable v -> modifyTVar availableResources (v :)
      CreationTicket -> modifyTVar' createdResourceCount (subtract 1)
    Just w -> putTMVar w c
{-# INLINE returnResourceSTM' #-}

popAvailable :: Pool a -> STM (Maybe (PoolEntry a))
popAvailable Pool {availableResources} = do
  readTVar availableResources >>= \case
    [] -> pure Nothing
    x : xs -> do
      writeTVar availableResources xs
      pure (Just x)
{-# INLINE popAvailable #-}

-- Destroy a resource by calling its release action (ignoring exceptions), then returning a creation ticket to the pool.
--
-- We ignore release action exceptions because there are only two circumstances under which we destroy resources:
--
--   - A user callback threw an exception; we consider that exception more important.
--   - The reaper is releasing a resource after it's been idle for `resourceExpiry`.
--   - We are destroying the pool, so we don't care about release action exceptions.
--
-- This action never throws a synchronous exception.
destroyResource :: Pool a -> a -> IO ()
destroyResource Pool {releaseResource, awaitingResources, availableResources, createdResourceCount} c = do
  destroyResource' releaseResource awaitingResources availableResources createdResourceCount c
{-# INLINE destroyResource #-}

destroyResource' ::
  (a -> IO ()) ->
  Fsifo (TMVar (ResourceNotification a)) ->
  TVar [PoolEntry a] ->
  TVar Int ->
  a ->
  IO ()
destroyResource' release waiters rs crc r = do
  release r `catchAllSynchronous` \_ -> pure ()
  atomically (returnResourceSTM' waiters rs crc CreationTicket)
{-# INLINE destroyResource' #-}

-- | Run an action with a resource, then return the resource to the
-- pool.
--
-- === __Details__
--
-- If an unused resource is in the pool, then it is used. Otherwise,
-- if the resource limit is not yet met a resource is created to
-- provide to the action. Otherwise, block until a resource is
-- returned.
--
-- If an exception is thrown during the execution of the action then
-- the resource is released.
--
-- If no resources are available and the maximum number of live
-- resources has been reached, then @withResource@ blocks until a
-- resource is returned.
withResource ::
  Pool a ->
  (a -> IO b) ->
  IO b
withResource p k = do
  mask \restore -> do
    c <- takeResource p
    r <- restore (k c) `onException` destroyResource p c
    returnResource p c
    pure r
{-# INLINEABLE withResource #-}

takeResource :: Pool a -> IO a
takeResource p@Pool {createdResourceCount, maxResourceCount, acquireResource, awaitingResources} =
  (join . atomically) do
    popAvailable p >>= \case
      Just entry -> pure (pure (entryValue entry))
      Nothing -> do
        resourceCount <- readTVar createdResourceCount
        case resourceCount < maxResourceCount of
          True -> do
            writeTVar createdResourceCount $! resourceCount + 1
            pure acquire
          False -> do
            var <- newEmptyTMVar
            removeSelf <- pushFsifo awaitingResources var
            pure do
              notification <-
                atomically (takeTMVar var) `onException` do
                  -- We got hit with an async exception while waiting for a resource, so remove ourselves from the
                  -- waiters queue. If someone happened to give us a resource before we manage to do so, return it to
                  -- the pool.
                  atomically do
                    _ <- removeSelf
                    tryTakeTMVar var >>= \case
                      Nothing -> pure ()
                      Just v -> returnResourceSTM p v
              case notification of
                ResourceAvailable x -> pure (entryValue x)
                CreationTicket -> acquire
  where
    -- Attempt to acquire a resource. If that fails, return a creation ticket to the pool, which will allow the next
    -- taker to try without bumping the `createdResourceCount`.
    acquire = acquireResource `onException` atomically (returnResourceSTM p CreationTicket)
{-# INLINE takeResource #-}

returnResource :: Pool a -> a -> IO ()
returnResource pool resource = do
  ct <- getCurrentTime
  atomically (returnResourceSTM pool (ResourceAvailable (PoolEntry ct resource)))
{-# INLINE returnResource #-}

newtype Nanoseconds
  = Nanoseconds Word64
  deriving newtype (Num, Eq, Ord, Real, Enum, Integral)

newtype Milliseconds
  = Milliseconds Word64
  deriving newtype (Eq, Ord)

newtype Microseconds
  = Microseconds Int
  deriving newtype (Num, Eq, Ord, Real, Enum, Integral)

ms2ns :: Milliseconds -> Nanoseconds
ms2ns (Milliseconds x) =
  let ns = x * 1_000_000
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

getCurrentTime :: IO Nanoseconds
getCurrentTime = coerce getMonotonicTimeNSec

-- Control.Concurrent.forkIO without the default exception handler
forkIO :: IO () -> IO ThreadId
forkIO (IO action) =
  IO \s0 ->
    case fork# action s0 of
      (# s1, tid #) -> (# s1, ThreadId tid #)

catchAllSynchronous :: IO a -> (SomeException -> IO a) -> IO a
catchAllSynchronous action handler =
  action `catch` \e ->
    case fromException e of
      Just (SomeAsyncException _) -> throwIO e
      _ -> handler e
