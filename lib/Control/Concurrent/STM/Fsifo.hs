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
--   * \(\mathcal{O}(1)\) push
--   * \(\mathcal{O}(1)\) pop
--   * \(\mathcal{O}(1)\) delete
data Fsifo a
  = Fsifo
       -- Pop end (see below)
       -- Invariant: if queue is non-empty, points at a node whose back-pointer points right back here
      {-# UNPACK #-} !(LinkedListP a)
      -- Push end (see below)
      -- Invariant: points at the one pointer that's pointing at End
      {-# UNPACK #-} !(LinkedListPP a)

-- A Fsifo containing elements [1, 2, 3] is depicted by the following diagram:
--
--              +-----+-----+-----+          +-----+-----+-----+          +-----+-----+-----+
--     +-> Node |     |  1  |   ------> Node |     |  2  |   ------> Node |     |  3  |   ------> End
--     |        +-|---+-----+-----+          +--|--+-----+-----+          +--|--+-----+-----+
--     |          |            ^                |           ^                |           ^
--     |          |            |                |           |                |           |
--     |          |            +----------------+           +----------------+           |
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
-- node, just a pointer to a node.
--
-- When an element is either popped or deleted, in addition to adjusting the surrounding nodes' backwards- and forwards-
-- pointers in the straightforward way, as well as the queue's pop and push end as necessary, the node itself is
-- contorted to look like this:
--
--          +-----+-----+-----+
--     Node |     |     |   ------>
--          +-|---+-----+-----+
--            |            ^
--            |            |
--            +------------+
--
-- That way, we can answer the question "has this node already been popped/deleted?" with "iff its back-pointer points
-- to its forward-pointer". This lets us avoid doing anything if the delete action is called twice, or called after pop.

data LinkedList a
  = Node
      {-# UNPACK #-} !(LinkedListPP a) -- back-pointer
      a -- value
      {-# UNPACK #-} !(LinkedListP a) -- forward-pointer
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

-- | Create a queue in @IO@.
newFsifoIO :: IO (Fsifo a)
newFsifoIO = do
  pop <- newTVarIO End
  push <- newTVarIO pop
  pure (Fsifo pop push)
{-# INLINEABLE newFsifoIO #-}

-- | Push an element onto a queue.
--
-- @pushFsifo@ returns an action that attempts to delete the element from
-- the queue.
--
-- The action returns:
--
-- * @True@ if the element was successfully deleted from the queue
--
-- * @False@ if the element had already been popped or deleted from the queue
pushFsifo :: Fsifo a -> a -> STM (STM Bool)
pushFsifo (Fsifo _pop push) lbjVal = do
  -- In these variable names,
  --   "jfk" refers to the old latest element (before this push)
  --   "lbj" refers to the new latest element (this push)
  -- referring to the US president JFK who was succeeded by LBJ
  jfkForward <- readTVar push
  lbjBack <- newTVar jfkForward
  lbjForward <- newTVar End
  writeTVar jfkForward (Node lbjBack lbjVal lbjForward)
  writeTVar push lbjForward
  pure (maybeDeleteSelf push lbjBack lbjForward)
{-# INLINEABLE pushFsifo #-}

-- | Pop an element from a queue.
popFsifo :: Fsifo a -> STM (Maybe a)
popFsifo (Fsifo pop push) = do
  readTVar pop >>= \case
    End -> pure Nothing
    Node back val forward -> do
      deleteSelf push back pop forward
      pure (Just val)
{-# INLINEABLE popFsifo #-}

maybeDeleteSelf ::
  -- The queue's push end
  LinkedListPP a ->
  -- Our back pointer
  LinkedListPP a ->
  -- Our forward pointter
  LinkedListP a ->
  STM Bool
maybeDeleteSelf push back forward = do
  prevForward <- readTVar back
  -- If our back pointer points to our forward pointer then we have
  -- already been deleted from the queue
  case prevForward == forward of
    True -> pure False
    False -> do
      deleteSelf push back prevForward forward
      pure True
{-# INLINE maybeDeleteSelf #-}

-- Like maybeDeleteSelf, but doesn't check whether or not we have
-- already been deleted.
deleteSelf ::
  -- The queue's push end
  LinkedListPP a ->
  -- Our back pointer
  LinkedListPP a ->
  -- Our back pointer's contents, the previous node's forward pointer
  LinkedListP a ->
  -- Our forward pointer
  LinkedListP a ->
  STM ()
deleteSelf push back prevForward forward = do
  next <- readTVar forward
  writeTVar prevForward next
  case next of
    End -> writeTVar push prevForward
    Node nextBack _ _ -> writeTVar nextBack prevForward
  -- point the back pointer to the forward pointer as a sign that
  -- the cell has been deleted (referenced in maybeDeleteSelf)
  writeTVar back forward
{-# INLINE deleteSelf #-}
