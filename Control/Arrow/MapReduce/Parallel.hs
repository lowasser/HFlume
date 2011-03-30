{-# LANGUAGE Rank2Types, TupleSections #-}
module Control.Arrow.MapReduce.Parallel (MRParallel, runMRParallel) where

import Control.Arrow.MapReduce.Class
import Control.Arrow.MapReduce.Types
import Control.Arrow.MapReduce.Sharder
import Control.Arrow.MapReduce.KeySplitter.Linked

import Control.Category
import Control.Cofunctor
import Control.Arrow
import Control.Monad hiding (replicateM)
import Control.Exception
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Concurrent.Chan.Endable
import Control.Concurrent.Barrier

import Control.Sink.Class
import Control.Source.Class

import GHC.Conc

import Data.Vector (Vector)
import qualified Data.Vector as V

import Prelude hiding ((.), unzip)

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

instance Category MRParallel where
  id = MRParallel $ \ input output -> do
    flag <- newEmptyMVar
    forkIO $ do
      mapSourceM_ (emit output) input
      reportEnd output
      putMVar flag ()
    return (takeMVar flag)
  MRParallel f . MRParallel g = MRParallel $ \ input output -> do
    (gOut, fIn) <- newPipe
    gDone <- g input gOut
    fDone <- f fIn output
    return (gDone >> fDone)

wrap :: (a -> b) -> (c -> d) -> MRParallel b c -> MRParallel a d
wrap f g (MRParallel run) = MRParallel $ \ input output ->
  run (fmap (fmap f) input) (cofmap (fmap g) output)

instance Arrow MRParallel where
  arr f = MRParallel $ \ input output -> do -- not strict
    flag <- newEmptyMVar
    forkIO $ do
      mapSourceM_ (emit output . fmap f) input
      reportEnd output
      putMVar flag ()
    return (takeMVar flag)
  first (MRParallel run) = MRParallel $ \ input output ->
    let runSource = fmap (\ (x, (a, b)) -> ((x, b), a)) input
	runOutput = cofmap (\ ((x, b), a) -> (x, (a, b))) output
    in run runSource runOutput
  second (MRParallel run) = MRParallel $ \ input output ->
    let runSource = fmap (\ (x, (a, b)) -> ((x, a), b)) input
	runOutput = cofmap (\ ((x, a), b) -> (x, (a, b))) output
    in run runSource runOutput

instance ArrowChoice MRParallel where
  left (MRParallel run) = MRParallel $ \ input output -> do
    inSem <- newEmptyMVar
    (leftOut, runSource) <- newPipe
    (outL, outR) <- fan output
    let runOutput = cofmap (fmap Left) outL
    forkIO $ do
      mapSourceM_ (\ (x, i) -> case i of
	Left a	-> emit leftOut (x, a)
	Right b	-> emit outR (x, Right b)) input
      reportEnd outR
      putMVar inSem ()
    leftTerm <- run runSource runOutput
    return (takeMVar inSem >> leftTerm)
  right (MRParallel run) = MRParallel $ \ input output -> do
    inSem <- newEmptyMVar
    (rightOut, runSource) <- newPipe
    (outL, outR) <- fan output
    let runOutput = cofmap (fmap Right) outR
    forkIO $ do
      mapSourceM_ (\ (x, i) -> case i of
	Right a	-> emit rightOut (x, a)
	Left b	-> emit outL (x, Left b)) input
      reportEnd outL
      putMVar inSem ()
    rightTerm <- run runSource runOutput
    return (takeMVar inSem >> rightTerm)
  MRParallel runLeft +++ MRParallel runRight = MRParallel $ \ input output -> do
    inSem <- newEmptyMVar
    (leftPipe, leftIn) <- newPipe
    (rightPipe, rightIn) <- newPipe
    (leftOut0, rightOut0) <- fan output
    let leftOut = cofmap (fmap Left) leftOut0
	rightOut = cofmap (fmap Right) rightOut0
    forkIO $ do
      mapSourceM_ (\ (x, i) -> case i of
	Left a	-> emit leftPipe (x, a)
	Right b	-> emit rightPipe (x, b)) input
      putMVar inSem ()
    leftTerm <- runLeft leftIn leftOut
    rightTerm <- runRight rightIn rightOut
    return (takeMVar inSem >> leftTerm >> rightTerm)
  MRParallel runLeft ||| MRParallel runRight = MRParallel $ \ input output -> do
    inSem <- newEmptyMVar
    (leftPipe, leftIn) <- newPipe
    (rightPipe, rightIn) <- newPipe
    (leftOut, rightOut) <- fan output
    forkIO $ do
      mapSourceM_ (\ (x, i) -> case i of
	Left a	-> emit leftPipe (x, a)
	Right b	-> emit rightPipe (x, b)) input
      putMVar inSem ()
    leftTerm <- runLeft leftIn leftOut
    rightTerm <- runRight rightIn rightOut
    return (takeMVar inSem >> leftTerm >> rightTerm)

instance ArrowZero MRParallel where
  zeroArrow = MRParallel $ \ _ _ -> return (return ())

instance ArrowPlus MRParallel where
  MRParallel run1 <+> MRParallel run2 = MRParallel $ \ input output -> do
    (pipe1, in1) <- newPipe
    (pipe2, in2) <- newPipe
    inSem <- newEmptyMVar
    forkIO $ do
      mapSourceM_ (\ a -> emit pipe1 a >> emit pipe2 a) input
      putMVar inSem ()
    (out1, out2) <- fan output
    term1 <- run1 in1 out1
    term2 <- run2 in2 out2
    return (takeMVar inSem >> term1 >> term2)

mapper :: Int -> Mapper a k b -> MRParallel a (k, b)
mapper nMappers theMap = MRParallel $ \ input output -> do
  sem <- newEmptyMVar
  outputs <- fanN nMappers output
  forM_ outputs $ \ myOut -> forkIO $ do
    theMap input myOut
    putMVar sem ()
  return (replicateM_ nMappers (takeMVar sem))

reducer :: Eq k => Reducer k a b -> MRParallel (k, a) b
reducer theReduce = MRParallel $ \ input output -> do
  barrier <- newBarrier
  addBarrier barrier (reportEnd output)
  shardOut <- newKeySplitter $ \ k -> do
    (kSnk, kSrc) <- newPipe
    kOut <- fan' output
    kSem <- newEmptyMVar
    addBarrier barrier (takeMVar kSem)
    forkIO $ do
      theReduce k kSrc kOut
      putMVar kSem ()
    return kSnk
  sem <- newEmptyMVar
  addBarrier barrier (takeMVar sem)
  forkIO $ do
    mapSourceM_ (emit shardOut) input
    reportEnd shardOut
    putMVar sem ()
  return (waitForBarrier barrier)