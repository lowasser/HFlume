{-# LANGUAGE Rank2Types, TupleSections, FlexibleInstances, MultiParamTypeClasses #-}
module Control.Arrow.MapReduce.Parallel (MRParallel, runMRParallel) where

import Control.Arrow.MapReduce.Class
import Control.Arrow.MapReduce.Types
import Control.Arrow.MapReduce.Sharder
import Control.Arrow.MapReduce.KeySplitter.Linked

import Control.Category
import Control.Cofunctor
import Control.Arrow
import Control.Monad
import Control.Exception
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Concurrent.Chan.Endable
import Control.Concurrent.Barrier

import Control.Sink.Class
import Control.Source.Class

import GHC.Conc

import Data.Hashable
import Data.Vector (Vector)
import qualified Data.Vector as V

import Prelude hiding ((.))

newtype MRParallel input output = MRParallel {run :: forall x . 
  MRSource (x, input)
  -> MRSink (x, output)
  -> IO (IO ())} -- returns a "wait till done" command

runMRParallel :: MRParallel a b -> [a] -> IO [b]
runMRParallel m xs = do
  (writeIn0, readIn) <- newPipe
  (writeOut, readOut0) <- newPipe
  let writeIn = cofmap ((),) writeIn0
  let readOut = fmap snd readOut0
  finish <- run m readIn writeOut
  mapM_ (emit writeIn) xs
  reportEnd writeIn
  finish
  consumeSource readOut

instance Functor (MRParallel a) where
  fmap f m = MRParallel $ \ input output ->
    run m input (cofmap (fmap f) output)

instance Category MRParallel where
  id = MRParallel $ \ input output -> 
    liftM (>> reportEnd output) $ workers (emit output) input
  MRParallel f . MRParallel g = MRParallel $ \ input output -> do
    (gOut, fIn) <- newPipe
    gDone <- g input gOut
    fDone <- f fIn output
    return (gDone >> fDone)

wrap :: (a -> b) -> (c -> d) -> MRParallel b c -> MRParallel a d
wrap f g m = MRParallel $ \ input output ->
  run m (fmap (fmap f) input) (cofmap (fmap g) output)

instance Arrow MRParallel where
  arr f = MRParallel $ \ input output -> liftM (>> reportEnd output) $ workers (emit output . fmap f) input
  first (MRParallel run) = MRParallel $ \ input output ->
    let runSource = fmap (\ (x, (a, b)) -> ((x, b), a)) input
	runOutput = cofmap (\ ((x, b), a) -> (x, (a, b))) output
    in run runSource runOutput
  second (MRParallel run) = MRParallel $ \ input output ->
    let runSource = fmap (\ (x, (a, b)) -> ((x, a), b)) input
	runOutput = cofmap (\ ((x, a), b) -> (x, (a, b))) output
    in run runSource runOutput

instance ArrowChoice MRParallel where
  left m = MRParallel $ \ input output -> do
    (leftPipe, leftIn) <- newPipe
    (outL, outR) <- fan output
    inTerm <- workers (\ (x, i) -> case i of
	Left a	-> emit leftPipe (x, a)
	Right b	-> emit outR (x, Right b)) input
    mTerm <- run (fmap Left m) leftIn outL
    return $ sequence_ [inTerm, reportEnd leftPipe, reportEnd outR, mTerm]
  right m = MRParallel $ \ input output -> do
    (rightPipe, rightIn) <- newPipe
    (outL, outR) <- fan output
    inTerm <- workers (\ (x, i) -> case i of
	Right a	-> emit rightPipe (x, a)
	Left b	-> emit outL (x, Left b)) input
    mTerm <- run (fmap Right m) rightIn outR
    return $ sequence_ [inTerm, reportEnd rightPipe, reportEnd outL, mTerm]
  l +++ r = fmap Left l ||| fmap Right r
  MRParallel runLeft ||| MRParallel runRight = MRParallel $ \ input output -> do
    (leftPipe, leftIn) <- newPipe
    (rightPipe, rightIn) <- newPipe
    (leftOut, rightOut) <- fan output
    inTerm <- workers (\ (x, i) -> case i of
	Left a	-> emit leftPipe (x, a)
	Right b	-> emit rightPipe (x, b)) input
    leftTerm <- runLeft leftIn leftOut
    rightTerm <- runRight rightIn rightOut
    return (sequence_ [inTerm, reportEnd leftPipe, reportEnd rightPipe, leftTerm, rightTerm])

instance ArrowZero MRParallel where
  zeroArrow = MRParallel $ \ _ _ -> return (return ())

instance ArrowPlus MRParallel where
  m <+> k = asum [m, k]

asum :: [MRParallel a b] -> MRParallel a b
asum pipelines = MRParallel $ \ input output -> do
  (pipes, terms) <- liftM unzip $ forM pipelines $ \ pipeline -> do
    (pipe, lineIn) <- newPipe
    lineOut <- fan' output
    term <- run pipeline lineIn lineOut
    return (pipe, term)
  inTerm <- workers (\ a -> mapM_ (`emit` a) pipes) input
  return $ sequence_ [inTerm, mapM_ reportEnd pipes, sequence_ terms, reportEnd output]

mapper :: Int -> Mapper a k b -> MRParallel a (k, b)
mapper nMappers theMap = MRParallel $ \ input output -> do
  sem <- newEmptyMVar
  forM_ [0..nMappers-1] $ flip forkOnIO $ do
    myOut <- fan' output
    theMap input myOut
    putMVar sem ()
  return (replicateM_ nMappers (takeMVar sem) >> reportEnd output)

instance (Eq k, Hashable k) => ArrowMapReduce MRParallel k where
  mapManyReduce buckets theMapper theReducer = MRParallel $ \ input output -> do
    (shardPipes, shardBarriers) <- liftM V.unzip $ V.replicateM buckets (makeReducerSplitter theReducer output)
    mapTerm <- run (mapper numCapabilities theMapper) input (MRSink $ Sharder shardPipes)
    return (mapTerm >> V.mapM_ waitForBarrier shardBarriers >> reportEnd output)

makeReducerSplitter :: Eq k => Reducer k a b -> MRSink (x, b) -> IO (MRSink (x, (k, a)), Barrier)
makeReducerSplitter theReduce output = do
  barrier <- newBarrier
  splitter <- newKeySplitter $ \ k -> do
    (kSnk, kSrc) <- newPipe
    kOut <- fan' output
    kSem <- newEmptyMVar
    addBarrier barrier (takeMVar kSem)
    forkIO $ do
      theReduce k kSrc kOut
      putMVar kSem ()
    return kSnk
  return (MRSink splitter, barrier)