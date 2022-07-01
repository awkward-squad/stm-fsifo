{-# LANGUAGE LambdaCase #-}

module Control.Concurrent.STM.Fsifo
  ( Fsifo,
    newFsifo,
    newFsifoIO,
    push,
    pop,
  )
where

import Control.Concurrent.STM
import Data.Functor

-- | A first-/still/-in-first-out queue that supports
--
--   * \( O(1) \) push
--   * \( O(1) \) pop
--   * \( O(1) \) delete
data Fsifo a
  = Fsifo
      {-# UNPACK #-} !(TVar (TDList a))
      -- ^ The head of the list
      {-# UNPACK #-} !(TVar (TVar (TDList a)))
      -- ^ Pointer to the final forward pointer in the list

-- This is similar to a double-ended doubly-linked queue, but employs
-- a few simplifications afforded by the limited interface.

-- | Each element has a pointer to the previous element's forward
-- pointer where "previous element" can be a 'TDList' or the 'Fsifo'
-- head pointer.
data TDList a
  = TCons
      {-# UNPACK #-} !(TVar (TVar (TDList a)))
      a
      {-# UNPACK #-} !(TVar (TDList a))
  | TNil

-- | Create a @Fsifo@
newFsifo :: STM (Fsifo a)
newFsifo = do
  emptyVarL <- newTVar TNil
  emptyVarR <- newTVar emptyVarL
  pure (Fsifo emptyVarL emptyVarR)
{-# INLINEABLE newFsifo #-}

-- | Create a @Fsifo@ in @IO@
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
  STM Bool
maybeRemoveSelf tv prevPP nextP = do
  prevP <- readTVar prevPP
  -- If our back pointer points to our forward pointer then we have
  -- already been removed from the queue
  case prevP == nextP of
    True -> pure False
    False -> removeSelf tv prevPP prevP nextP $> True
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

-- | Push an element onto a queue.
--
-- @push@ returns an action that attempts to remove the element from
-- the queue.
--
-- The action returns:
--
-- * @True@ if the element was removed from the queue
--
-- * @False@ if the element was discovered to be no longer in the queue
push :: Fsifo a -> a -> STM (STM Bool)
push (Fsifo _ tv) a = do
  fwdPointer <- readTVar tv
  backPointer <- newTVar fwdPointer
  emptyVar <- newTVar TNil
  let cell = TCons backPointer a emptyVar
  writeTVar fwdPointer cell
  writeTVar tv emptyVar
  pure (maybeRemoveSelf tv backPointer emptyVar)
{-# INLINEABLE push #-}

-- | Pop an element from a queue.
pop :: Fsifo a -> STM (Maybe a)
pop (Fsifo hv tv) = do
  readTVar hv >>= \case
    TNil -> pure Nothing
    TCons bp a fp -> do
      removeSelf tv bp hv fp
      pure (Just a)
{-# INLINEABLE pop #-}
