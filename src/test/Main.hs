{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IM
import Data.Kind
import Data.Maybe
import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import qualified Pool.Queue as Pool
import Test.Tasty
import Test.Tasty.Hedgehog

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
  qvar <- liftIO $ newTVarIO =<< Pool.newTDQueueIO
  actions <-
    forAll $
      Gen.sequential
        (Range.linear 1 100)
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

    resetQ qvar = atomically do
      q <- Pool.newTDQueue
      writeTVar qvar q

    s_push qvar =
      let gen QueueModel {..} =
            case isChecked of
              True -> Just (Push <$> Gen.integral Range.constantBounded)
              False -> Nothing
          execute (Push n) = do
            DeleteFromQueue
              <$> liftIO
                ( atomically do
                    q <- readTVar qvar
                    Pool.push q n
                )
          upd = Update \(QueueModel s pushCount im _) (Push i) out ->
            let !pushCount' = pushCount + 1
                im' = IM.insert pushCount' out im
                s' = IM.insert pushCount' i s
             in QueueModel s' pushCount' im' False
          preCond = Require \QueueModel {..} _ -> isChecked
       in Command gen execute [upd, preCond]

    s_pop qvar =
      let gen QueueModel {..} =
            case isChecked of
              True -> Just (pure Pop)
              False -> Nothing
          execute Pop = do
            liftIO $ atomically do
              q <- readTVar qvar
              Pool.pop q
          upd = Update \(QueueModel s pushCount im _) Pop _out ->
            let mk = fst <$> IM.lookupGE minBound s
                s' = maybe s (\k -> IM.delete k s) mk
             in QueueModel s' pushCount im False
          precond = Require \QueueModel {..} _ -> isChecked
          postcond = Ensure \QueueModel {..} _ _ actualPop ->
            let smallestElem = snd <$> IM.lookupGE minBound queueRep
             in smallestElem === actualPop
       in Command gen execute [upd, precond, postcond]

    s_removeSelf _ =
      let gen QueueModel {..} =
            case isChecked && pushCount >= 0 of
              True -> Just do
                i <- Gen.integral (Range.constant 0 pushCount)
                pure (RemoveSelf i (fromJust (IM.lookup i removeSelfMap)))
              False -> Nothing
          execute (RemoveSelf _ (Var (Concrete (DeleteFromQueue stm)))) = do
            liftIO (atomically stm)
          upd = Update \(QueueModel s pushCount im _) (RemoveSelf i _) _ ->
            QueueModel (IM.delete i s) pushCount im False
          preCond = Require \QueueModel {..} _ -> isChecked
       in Command gen execute [upd, preCond]

    s_check qvar =
      let gen QueueModel {..} =
            case isChecked of
              True -> Nothing
              False -> Just (pure Check)
          execute Check = do
            liftIO (atomically $ Pool.toList =<< readTVar qvar)
          upd = Update \(QueueModel im s pushCount _) Check _ ->
            QueueModel im s pushCount True
          postcond = Ensure \_ QueueModel {..} Check xs ->
            xs === map snd (IM.toAscList queueRep)
          precond = Require \QueueModel {..} _ -> not isChecked
       in Command gen execute [upd, precond, postcond]

-- A newtype for our STM delete action since a Show instance is
-- required for outputs
newtype DeleteFromQueue = DeleteFromQueue (STM ())

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
  deriving (Show, Eq)

instance HTraversable Push where
  htraverse _ (Push x) = pure (Push x)

data Check (v :: Type -> Type) = Check
  deriving (Show, Eq)

instance HTraversable Check where
  htraverse _ Check = pure Check

data Pop (v :: Type -> Type) = Pop
  deriving (Show, Eq)

instance HTraversable Pop where
  htraverse _ Pop = pure Pop

data RemoveSelf (v :: Type -> Type) = RemoveSelf Int (Var DeleteFromQueue v)
  deriving (Show)

instance HTraversable RemoveSelf where
  htraverse eta (RemoveSelf x v) = RemoveSelf x <$> htraverse eta v
