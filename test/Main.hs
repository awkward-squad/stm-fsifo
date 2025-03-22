{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedRecordDot #-}

module Main (main) where

import Control.Applicative (optional)
import Control.Concurrent.STM
import Control.Concurrent.STM.TokenQueue qualified as TokenQueue
import Control.Monad.IO.Class
import Data.IntMap (IntMap)
import Data.IntMap.Strict qualified as IM
import Data.Kind
import Data.Maybe
import GHC.Generics (Generic)
import Hedgehog
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty
import Test.Tasty.Hedgehog
import Unsafe.Coerce (unsafeCoerce)

main :: IO ()
main =
  defaultMain $
    testGroup
      "Tests"
      [ testGroup
          "queue"
          [ testProperty "behaves as expected" prop_queue
          ]
      ]

prop_queue :: Property
prop_queue = property do
  qvar <- liftIO $ newTVarIO =<< TokenQueue.newIO
  actions <-
    forAll $
      Gen.sequential
        (Range.linear 1 10000)
        initialState
        [ s_push qvar,
          s_check qvar,
          s_removeSelf qvar,
          s_pop qvar
        ]
  evalIO (resetQ qvar)
  executeSequential initialState actions
  where
    initialState =
      QueueModel
        { queueRep = mempty,
          pushCount = -1,
          removeSelfMap = mempty,
          isChecked = True
        }

    resetQ qvar =
      atomically do
        q <- TokenQueue.new
        writeTVar qvar q

    s_push qvar =
      let gen model =
            case model.isChecked of
              True -> Just (Push <$> Gen.integral Range.constantBounded)
              False -> Nothing
          execute (Push n) = do
            DeleteFromQueue
              <$> liftIO
                ( atomically do
                    q <- readTVar qvar
                    TokenQueue.push q n
                )
          upd = Update \(QueueModel s pushCount im _) (Push i) out ->
            let !pushCount' = pushCount + 1
                im' = IM.insert pushCount' out im
                s' = IM.insert pushCount' i s
             in QueueModel s' pushCount' im' False
          preCond = Require \model _ -> model.isChecked
       in Command gen execute [upd, preCond]

    s_pop qvar =
      let gen model =
            case model.isChecked of
              True -> Just (pure Pop)
              False -> Nothing
          execute Pop = do
            liftIO $ atomically do
              q <- readTVar qvar
              optional (TokenQueue.pop q)
          upd = Update \(QueueModel s pushCount im _) Pop _out ->
            let mk = fst <$> IM.lookupGE minBound s
                s' = maybe s (\k -> IM.delete k s) mk
             in QueueModel s' pushCount im False
          precond = Require \model _ -> model.isChecked
          postcond = Ensure \model _ _ actualPop ->
            let smallestElem = snd <$> IM.lookupGE minBound model.queueRep
             in smallestElem === actualPop
       in Command gen execute [upd, precond, postcond]

    s_removeSelf _ =
      let gen model =
            case model.isChecked && model.pushCount >= 0 of
              True -> Just do
                i <- Gen.integral (Range.constant 0 model.pushCount)
                pure (RemoveSelf i (fromJust (IM.lookup i model.removeSelfMap)))
              False -> Nothing
          execute (RemoveSelf _ (Var (Concrete (DeleteFromQueue token)))) = do
            liftIO (print =<< atomically (TokenQueue.delete token))
          upd = Update \(QueueModel s pushCount im _) (RemoveSelf i _) _ ->
            QueueModel (IM.delete i s) pushCount im False
          preCond = Require \model _ -> model.isChecked
       in Command gen execute [upd, preCond]

    s_check qvar =
      let gen model =
            case model.isChecked of
              True -> Nothing
              False -> Just (pure Check)
          execute Check = do
            liftIO (atomically $ peekAll =<< readTVar qvar)
          upd = Update \(QueueModel im s pushCount _) Check _ ->
            QueueModel im s pushCount True
          postcond = Ensure \_ model Check xs ->
            xs === map snd (IM.toAscList model.queueRep)
          precond = Require \model _ -> not model.isChecked
       in Command gen execute [upd, precond, postcond]

-- A newtype for our token since a Show instance is required for outputs
newtype DeleteFromQueue = DeleteFromQueue TokenQueue.Token

instance Show DeleteFromQueue where
  show _ = "<stm>"

data QueueModel (v :: Type -> Type) = QueueModel
  { -- We represent our queue by an IntMap where the key increments on
    -- every push and pops/deletions just delete from the map.
    queueRep :: IntMap Int,
    pushCount :: !Int,
    -- Whenever we push we add to the queueRep map but add the remove
    -- action to this map. When we delete we do not delete from this
    -- map, so that we may test running the returned delete action
    -- multiple times.
    removeSelfMap :: IntMap (Var DeleteFromQueue v),
    -- This boolean exists merely to ensure that we run 's_check'
    -- after every modification to the queue. I would prefer for
    -- 's_check' to not exist and to have that check as a post
    -- condition on all modifications, but that would require
    -- returning multiple results and that is difficult with
    -- hedgehog. (https://github.com/hedgehogqa/haskell-hedgehog/issues/113)
    isChecked :: !Bool
  }

data Push (v :: Type -> Type) = Push Int
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FunctorB, TraversableB)

data Check (v :: Type -> Type) = Check
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FunctorB, TraversableB)

data Pop (v :: Type -> Type) = Pop
  deriving stock (Show, Eq, Generic)
  deriving anyclass (FunctorB, TraversableB)

data RemoveSelf (v :: Type -> Type) = RemoveSelf Int (Var DeleteFromQueue v)
  deriving stock (Show, Generic)
  deriving anyclass (FunctorB, TraversableB)

peekAll :: TokenQueue.TokenQueue a -> STM [a]
peekAll = toList . unsafeCoerce

toList :: Q a -> STM [a]
toList (Q hv _) =
  let go xs v = do
        readTVar v >>= \case
          TNil -> pure (reverse xs)
          TCons _ x np -> go (x : xs) np
   in go [] hv

data Q a
  = Q
      -- | The head of the list
      {-# UNPACK #-} !(TVar (TDList a))
      -- | Pointer to the final forward pointer in the list
      {-# UNPACK #-} !(TVar (TVar (TDList a)))

-- | Each element has a pointer to the previous element's forward
-- pointer where "previous element" can be a 'TDList' or the 'TokenQueue'
-- head pointer.
data TDList a
  = TCons
      {-# UNPACK #-} !(TVar (TVar (TDList a)))
      a
      {-# UNPACK #-} !(TVar (TDList a))
  | TNil
