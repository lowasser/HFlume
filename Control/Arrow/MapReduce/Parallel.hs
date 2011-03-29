{-# LANGUAGE Rank2Types #-}
module Control.Arrow.MapReduce.Parallel (MRParallel) where

import Control.Arrow.MapReduce.Class
import Control.Arrow.MapReduce.Types
import Control.Arrow.MapReduce.Sharder

import Control.Input.Class
import Control.Output.Class

import Control.Category
import Control.Cofunctor
import Control.Arrow
import Control.Monad hiding (replicateM)
import Control.Exception
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Concurrent.Chan.Endable

import GHC.Conc

import Data.Vector
import qualified Data.Vector as V

import Prelude hiding ((.), unzip)

newtype MRParallel input output = MRParallel (forall x . 
  MRInput (x, input)
  -> MROutput (x, output)
  -> IO (IO ())) -- returns a "wait till done" command

instance Category MRParallel where
  id = MRParallel $ \ input output -> do
    flag <- newEmptyMVar
    forkIO $ do
      mapInputM_ (emit output) input
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
      mapInputM_ (emit output . fmap f) input
      putMVar flag ()
    return (takeMVar flag)
  first (MRParallel run) = MRParallel $ \ input output ->
    let runInput = fmap (\ (x, (a, b)) -> ((x, b), a)) input
	runOutput = cofmap (\ ((x, b), a) -> (x, (a, b))) output
    in run runInput runOutput
  second (MRParallel run) = MRParallel $ \ input output ->
    let runInput = fmap (\ (x, (a, b)) -> ((x, a), b)) input
	runOutput = cofmap (\ ((x, a), b) -> (x, (a, b))) output
    in run runInput runOutput

instance ArrowChoice MRParallel where
  left (MRParallel run) = MRParallel $ \ input output -> do
    inSem <- newEmptyMVar
    (leftOut, runInput) <- newPipe
    let runOutput = cofmap (fmap Left) output
    forkIO $ do
      mapInputM_ (\ (x, i) -> case i of
	Left a	-> emit leftOut (x, a)
	Right b	-> emit output (x, Right b)) input
      putMVar inSem ()
    leftTerm <- run runInput runOutput
    return (takeMVar inSem >> leftTerm)
  right (MRParallel run) = MRParallel $ \ input output -> do
    inSem <- newEmptyMVar
    (rightOut, runInput) <- newPipe
    let runOutput = cofmap (fmap Right) output
    forkIO $ do
      mapInputM_ (\ (x, i) -> case i of
	Right a	-> emit rightOut (x, a)
	Left b	-> emit output (x, Left b)) input
      putMVar inSem ()
    rightTerm <- run runInput runOutput
    return (takeMVar inSem >> rightTerm)
  MRParallel runLeft +++ MRParallel runRight = MRParallel $ \ input output -> do
    inSem <- newEmptyMVar
    (leftPipe, leftIn) <- newPipe
    (rightPipe, rightIn) <- newPipe
    let leftOut = cofmap (fmap Left) output
	rightOut = cofmap (fmap Right) output
    forkIO $ do
      mapInputM_ (\ (x, i) -> case i of
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
    forkIO $ do
      mapInputM_ (\ (x, i) -> case i of
	Left a	-> emit leftPipe (x, a)
	Right b	-> emit rightPipe (x, b)) input
      putMVar inSem ()
    leftTerm <- runLeft leftIn output
    rightTerm <- runRight rightIn output
    return (takeMVar inSem >> leftTerm >> rightTerm)

instance ArrowZero MRParallel where
  zeroArrow = MRParallel $ \ _ _ -> return (return ())

instance ArrowPlus MRParallel where
  MRParallel run1 <+> MRParallel run2 = MRParallel $ \ input output -> do
    (pipe1, in1) <- newPipe
    (pipe2, in2) <- newPipe
    inSem <- newEmptyMVar
    forkIO $ do
      mapInputM_ (\ a -> emit pipe1 a >> emit pipe2 a) input
      putMVar inSem ()
    term1 <- run1 in1 output
    term2 <- run2 in2 output
    return (takeMVar inSem >> term1 >> term2)