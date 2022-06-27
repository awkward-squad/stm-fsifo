{-# LANGUAGE LambdaCase #-}

module Control.Concurrent.STM.Fsifo
  ( Fsifo,
    newFsifo,
    newFsifoIO,
    push,
    pop,
    toList,
  )
where

import Control.Concurrent.STM

-- | A FIFO queue that supports /O(1)/ push, pop, and removing any
-- element from the queue.
--
-- This is similar to a double-ended doubly-linked queue, but employs
-- a few simplifications afforded by the limited interface.
data Fsifo a
  = Fsifo
      {-# UNPACK #-} !(TVar (TDList a))
      -- ^ The head of the list
      {-# UNPACK #-} !(TVar (TVar (TDList a)))
      -- ^ Pointer to the final forward pointer in the list

-- | Each element has a pointer to the previous element's forward
-- pointer where "previous element" can be a 'TDList' or the 'Fsifo'
-- head pointer.
data TDList a
  = TCons
      {-# UNPACK #-} !(TVar (TVar (TDList a)))
      a
      {-# UNPACK #-} !(TVar (TDList a))
  | TNil

newFsifo :: STM (Fsifo a)
newFsifo = do
  emptyVarL <- newTVar TNil
  emptyVarR <- newTVar emptyVarL
  pure (Fsifo emptyVarL emptyVarR)
{-# INLINEABLE newFsifo #-}

newFsifoIO :: IO (Fsifo a)
newFsifoIO = do
  emptyVarL <- newTVarIO TNil
  emptyVarR <- newTVarIO emptyVarL
  pure (Fsifo emptyVarL emptyVarR)
{-# INLINEABLE newFsifoIO #-}

maybeRemoveSelf ::
  -- | 'Fsifo's final foward pointer pointer
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

-- Like maybeRemoveSelf, but doesn't check whether or not we have
-- already been removed.
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
  -- point the back pointer to the forward pointer as a sign that
  -- the cell has been popped (referenced in maybeRemoveSelf)
  writeTVar prevPP nextP
{-# INLINE removeSelf #-}

-- | Push an element onto the back of the queue.
--
-- Returns an STM action that removes this element from the queue.
push :: Fsifo a -> a -> STM (STM ())
push (Fsifo _ tv) a = do
  fwdPointer <- readTVar tv
  backPointer <- newTVar fwdPointer
  emptyVar <- newTVar TNil
  let cell = TCons backPointer a emptyVar
  writeTVar fwdPointer cell
  writeTVar tv emptyVar
  pure (maybeRemoveSelf tv backPointer emptyVar)
{-# INLINEABLE push #-}

-- | Pop an element from the front of the queue.
pop :: Fsifo a -> STM (Maybe a)
pop (Fsifo hv tv) = do
  readTVar hv >>= \case
    TNil -> pure Nothing
    TCons bp a fp -> do
      removeSelf tv bp hv fp
      pure (Just a)
{-# INLINEABLE pop #-}

-- | Produce a list of the queue's elements.
--
-- This does not remove any elements from the queue.
toList :: Fsifo a -> STM [a]
toList (Fsifo hv _) =
  let go xs v = do
        readTVar v >>= \case
          TNil -> pure (reverse xs)
          TCons _ x np -> go (x : xs) np
   in go [] hv
{-# INLINEABLE toList #-}
