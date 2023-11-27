module Control.Concurrent.STM.Fsifo
  ( Fsifo,
    newFsifo,
    newFsifoIO,
    pushFsifo,
    popFsifo,
  )
where

import Control.Concurrent.STM (STM, TVar, newTVar, newTVarIO, readTVar, writeTVar)

-- | A first-/still/-in-first-out queue that supports
--
--   * \( O(1) \) push
--   * \( O(1) \) pop
--   * \( O(1) \) delete
data Fsifo a
  = Fsifo
      {-# UNPACK #-} !(LinkedListP a) -- Pop end (see below)
      {-# UNPACK #-} !(LinkedListPP a) -- Push end (see below)

-- A Fsifo containing elements [1, 2, 3] is depicted by the following diagram:
--
--
--              +-----+-----+-----+          +-----+-----+-----+          +-----+-----+-----+
--     +-> Node |     |  1  |   ------> Node |     |  2  |   ------> Node |     |  3  |   ------> End
--     |        +-|---+-----+-----+          +--|--+-----+-----+          +--|--+-----+-----+
--     |          |            ^                |           ^                |           ^
--     |          |            +----------------+           +----------------+           |
--     |          |                                                                      |
--     |          |                                                                      |
--     |          v                                                                      |
--     |     +-----+                                                                  +--|--+
--     +--------   |                                                                  |     |
--           +-----+                                                                  +-----+
--            Pop end                                                                  Push end
--
-- As you can see, it's a relatively standard doubly-linked structure with backwards- and forwards-pointers, except
-- each backwards pointer doesn't actually point at the entire previous node, but rather the previous node's forward
-- pointer.
--
-- This is simply done for efficiency; none of the supported operations (push, pop, delete) require being able to follow
-- a backwards pointer back to the entire previous node.
--
-- Also, this way, the first node's backwards pointer can conveniently point to the queue's read end, which is not a
-- node, just a pointer to a node ;)
--
-- FIXME: wait, why is that convenient?
--
-- FIXME: say more about how the structure is transformed during a delete

data LinkedList a
  = Node
      {-# UNPACK #-} !(LinkedListPP a)
      a
      {-# UNPACK #-} !(LinkedListP a)
  | End

-- Do these aliases help or hurt? :/

type LinkedListP a =
  TVar (LinkedList a)

type LinkedListPP a =
  TVar (LinkedListP a)

-- | Create a queue.
newFsifo :: STM (Fsifo a)
newFsifo = do
  pop <- newTVar End
  push <- newTVar pop
  pure (Fsifo pop push)
{-# INLINEABLE newFsifo #-}

-- | Create a @Fsifo@ in @IO@.
newFsifoIO :: IO (Fsifo a)
newFsifoIO = do
  pop <- newTVarIO End
  push <- newTVarIO pop
  pure (Fsifo pop push)
{-# INLINEABLE newFsifoIO #-}

-- | Push an element onto a queue.
--
-- @pushFsifo@ returns an action that attempts to remove the element from
-- the queue.
--
-- The action returns:
--
-- * @True@ if the element was removed from the queue
--
-- * @False@ if the element was discovered to be no longer in the queue
pushFsifo :: Fsifo a -> a -> STM (STM Bool)
pushFsifo (Fsifo _pop push) lbjVal = do
  -- In these variable names,
  --   "jfk" refers to the old latest element (before this push)
  --   "lbj" refers to the new latest element (this push)
  -- referring to the US presidents
  --   FDR -> Truman -> Eisenhower -> JFK -> LBJ
  jfkForward <- readTVar push
  lbjBack <- newTVar jfkForward
  lbjForward <- newTVar End
  writeTVar jfkForward (Node lbjBack lbjVal lbjForward)
  writeTVar push lbjForward
  pure (maybeRemoveSelf push lbjBack lbjForward)
{-# INLINEABLE pushFsifo #-}

-- | Pop an element from a queue.
popFsifo :: Fsifo a -> STM (Maybe a)
popFsifo (Fsifo pop push) = do
  readTVar pop >>= \case
    End -> pure Nothing
    Node backPP x forwardP -> do
      removeSelf push backPP pop forwardP
      pure (Just x)
{-# INLINEABLE popFsifo #-}

maybeRemoveSelf ::
  -- The queue's push end
  LinkedListPP a ->
  -- Our back pointer
  LinkedListPP a ->
  -- Our forward pointter
  LinkedListP a ->
  STM Bool
maybeRemoveSelf push backPP forwardP = do
  backP <- readTVar backPP
  -- If our back pointer points to our forward pointer then we have
  -- already been removed from the queue
  case backP == forwardP of
    True -> pure False
    False -> do
      removeSelf push backPP backP forwardP
      pure True
{-# INLINE maybeRemoveSelf #-}

-- Like maybeRemoveSelf, but doesn't check whether or not we have
-- already been removed.
removeSelf ::
  -- The queue's push end
  LinkedListPP a ->
  -- Our back pointer
  LinkedListPP a ->
  -- Our back pointer's contents
  LinkedListP a ->
  -- Our forward pointter
  LinkedListP a ->
  STM ()
removeSelf push backPP backP forwardP = do
  forward <- readTVar forwardP
  writeTVar backP forward
  case forward of
    End -> writeTVar push backP
    Node forwardBackPP _ _ -> writeTVar forwardBackPP backP
  -- point the back pointer to the forward pointer as a sign that
  -- the cell has been popped (referenced in maybeRemoveSelf)
  writeTVar backPP forwardP
{-# INLINE removeSelf #-}
