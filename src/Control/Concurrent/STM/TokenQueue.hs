module Control.Concurrent.STM.TokenQueue
  ( TokenQueue,
    Token,
    new,
    newIO,
    push,
    push_,
    pop,
    peek,
    delete,
    delete_,
  )
where

import Control.Concurrent.STM (STM, TVar, newTVar, newTVarIO, readTVar, writeTVar)
import Control.Monad.STM (retry)
import Data.Coerce (coerce)
import Data.Functor (void)

-- | A "token queue" data structure that supports
--
--   * \(\mathcal{O}(1)\) push
--   * \(\mathcal{O}(1)\) pop
--   * \(\mathcal{O}(1)\) delete
data TokenQueue a
  = TokenQueue
      -- Pop end (see below)
      -- Invariant: if queue is non-empty, points at a node whose back-pointer points right back here
      {-# UNPACK #-} !(ListP a)
      -- Push end (see below)
      -- Invariant: points at the one pointer that's pointing at PushEnd
      {-# UNPACK #-} !(ListPP a)

-- A token queue containing elements [1, 2, 3] is depicted by the following diagram:
--
--          Node--+-----+-----+     Node--+-----+-----+     Node--+-----+-----+
--     +--> |     |  1  |   ------> |     |  2  |   ------> |     |  3  |   ------> End
--     |    +-|---+-----+-----+     +--|--+-----+-----+     +--|--+-----+-----+
--     |      |            ^           |           ^           |           ^
--     |      |            |           |           |           |           |
--     |      |            +-----------+           +-----------+           |
--     |      |                                                            |
--     |      v                                                            |
--     |   +-----+                                                      +--|--+
--     +------   |                                                      |     |
--         +-----+                                                      +-----+
--         Pop end                                                      Push end
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

data List a
  = Node
      {-# UNPACK #-} !(ListPP a) -- back-pointer
      a -- value
      {-# UNPACK #-} !(ListP a) -- forward-pointer
  | PushEnd

-- | A token associated with an element that was pushed onto a token queue, which can be used to attempt to delete the
-- element.
newtype Token
  = Token (STM Bool)

-- Do these aliases help or hurt? :/

type ListP a =
  TVar (List a)

type ListPP a =
  TVar (ListP a)

-- | Create an empty token queue.
new :: STM (TokenQueue a)
new = do
  popEnd <- newTVar PushEnd
  pushEnd <- newTVar popEnd
  pure (TokenQueue popEnd pushEnd)
{-# INLINEABLE new #-}

-- | Create an empty token queue in @IO@.
newIO :: IO (TokenQueue a)
newIO = do
  popEnd <- newTVarIO PushEnd
  pushEnd <- newTVarIO popEnd
  pure (TokenQueue popEnd pushEnd)
{-# INLINEABLE newIO #-}

-- | \(\mathcal{O}(1)\). Push an element onto a queue, and return a token that can be used to attempt to delete the element from the queue.
--
-- This function never blocks.
push :: TokenQueue a -> a -> STM Token
push (TokenQueue _ pushEnd) val = do
  prevForward <- readTVar pushEnd
  back <- newTVar prevForward
  forward <- newTVar PushEnd
  writeTVar prevForward (Node back val forward)
  writeTVar pushEnd forward
  pure (Token (maybeDeleteSelf pushEnd back forward))
{-# INLINEABLE push #-}

-- | \(\mathcal{O}(1)\). Like 'push', but for when its return value is not needed.
push_ :: TokenQueue a -> a -> STM ()
push_ queue val =
  void (push queue val)
{-# INLINE push_ #-}

-- | \(\mathcal{O}(1)\). Pop an element from a queue.
--
-- This function blocks if the queue is empty.
pop :: TokenQueue a -> STM a
pop (TokenQueue popEnd pushEnd) = do
  readTVar popEnd >>= \case
    PushEnd -> retry
    Node back val forward -> do
      deleteSelf pushEnd back popEnd forward
      pure val
{-# INLINEABLE pop #-}

-- | \(\mathcal{O}(1)\). Peek at the first element in a queue.
--
-- This function blocks if the queue is empty.
peek :: TokenQueue a -> STM a
peek (TokenQueue popEnd _) = do
  readTVar popEnd >>= \case
    PushEnd -> retry
    Node _ val _ -> pure val
{-# INLINEABLE peek #-}

-- | \(\mathcal{O}(1)\). Attempt to delete an element from a queue and return whether the delete was successful (where
-- 'False' indicates the element was no longer in the queue because it had either already been popped or deleted).
--
-- This function never blocks.
delete :: Token -> STM Bool
delete =
  coerce
{-# INLINE delete #-}

-- | \(\mathcal{O}(1)\). Like 'delete', but for when its return value is not needed.
delete_ :: Token -> STM ()
delete_ =
  void . delete
{-# INLINE delete_ #-}

maybeDeleteSelf ::
  -- The queue's push end
  ListPP a ->
  -- Our back pointer
  ListPP a ->
  -- Our forward pointter
  ListP a ->
  STM Bool
maybeDeleteSelf pushEnd back forward = do
  prevForward <- readTVar back
  -- If our back pointer points to our forward pointer then we have
  -- already been deleted from the queue
  case prevForward == forward of
    True -> pure False
    False -> do
      deleteSelf pushEnd back prevForward forward
      pure True
{-# INLINE maybeDeleteSelf #-}

-- Like maybeDeleteSelf, but doesn't check whether or not we have
-- already been deleted.
deleteSelf ::
  -- The queue's push end
  ListPP a ->
  -- Our back pointer
  ListPP a ->
  -- Our back pointer's contents, the previous node's forward pointer
  ListP a ->
  -- Our forward pointer
  ListP a ->
  STM ()
deleteSelf pushEnd back prevForward forward = do
  next <- readTVar forward
  writeTVar prevForward next
  case next of
    PushEnd -> writeTVar pushEnd prevForward
    Node nextBack _ _ -> writeTVar nextBack prevForward
  -- point the back pointer to the forward pointer as a sign that
  -- the cell has been deleted (referenced in maybeDeleteSelf)
  writeTVar back forward
{-# INLINE deleteSelf #-}
