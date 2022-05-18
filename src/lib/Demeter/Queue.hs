{-# LANGUAGE LambdaCase #-}

module Demeter.Queue
  ( TDQueue (..),
    newTDQueue,
    newTDQueueIO,
    push,
    pop,
    toList,
  )
where

import Control.Concurrent.STM

-- | A FIFO queue that supports removing any element from the queue.
--
-- We have a pointer to the head of the list, and also a pointer to
-- the final foward pointer in the list.
data TDQueue a
  = TDQueue
      {-# UNPACK #-} !(TVar (TDList a))
      {-# UNPACK #-} !(TVar (TVar (TDList a)))

-- | Each element has a pointer to the previous element's forward
-- pointer where "previous element" can be a 'TDList' or the 'TDQueue'
-- head pointer.
data TDList a
  = TCons
      {-# UNPACK #-} !(TVar (TVar (TDList a)))
      a
      {-# UNPACK #-} !(TVar (TDList a))
  | TNil

newTDQueue :: STM (TDQueue a)
newTDQueue = do
  emptyVarL <- newTVar TNil
  emptyVarR <- newTVar emptyVarL
  pure (TDQueue emptyVarL emptyVarR)
{-# INLINEABLE newTDQueue #-}

newTDQueueIO :: IO (TDQueue a)
newTDQueueIO = do
  emptyVarL <- newTVarIO TNil
  emptyVarR <- newTVarIO emptyVarL
  pure (TDQueue emptyVarL emptyVarR)
{-# INLINEABLE newTDQueueIO #-}

maybeRemoveSelf ::
  -- | 'TDQueue's final foward pointer pointer
  TVar (TVar (TDList a)) ->
  -- | Our back pointer
  TVar (TVar (TDList a)) ->
  -- | Our forward pointter
  TVar (TDList a) ->
  STM ()
maybeRemoveSelf tv prevPP nextP = do
  prevP <- readTVar prevPP
  -- If our back pointer points to our forward pointer then we have
  -- already been removed from the queue
  case prevP == nextP of
    True -> pure ()
    False -> removeSelf tv prevPP prevP nextP
{-# INLINE maybeRemoveSelf #-}

-- Like maybeRemoveSelf, but doesn't check whether or not we have already been removed.
removeSelf ::
  TVar (TVar (TDList a)) ->
  TVar (TVar (TDList a)) ->
  TVar (TDList a) ->
  TVar (TDList a) ->
  STM ()
removeSelf tv prevPP prevP nextP = do
  next <- readTVar nextP
  writeTVar prevP next
  case next of
    TNil -> writeTVar tv prevP
    TCons bp _ _ -> writeTVar bp prevP
  writeTVar prevPP nextP
{-# INLINE removeSelf #-}

-- | Returns an STM action that removes the pushed element from the
-- queue
push :: TDQueue a -> a -> STM (STM ())
push (TDQueue _ tv) a = do
  fwdPointer <- readTVar tv
  backPointer <- newTVar fwdPointer
  emptyVar <- newTVar TNil
  let cell = TCons backPointer a emptyVar
  writeTVar fwdPointer cell
  writeTVar tv emptyVar
  pure (maybeRemoveSelf tv backPointer emptyVar)
{-# INLINE push #-}

pop :: TDQueue a -> STM (Maybe a)
pop (TDQueue hv tv) = do
  readTVar hv >>= \case
    TNil -> pure Nothing
    TCons bp a fp -> do
      removeSelf tv bp hv fp
      pure (Just a)
{-# INLINE pop #-}

toList :: TDQueue a -> STM [a]
toList (TDQueue hv _) =
  let go xs v = do
        readTVar v >>= \case
          TNil -> pure (reverse xs)
          TCons _ x np -> go (x : xs) np
   in go [] hv
{-# INLINEABLE toList #-}
