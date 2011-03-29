{-# LANGUAGE Rank2Types #-}
module Control.Arrow.MapReduce.Parallel where

import Control.Arrow.MapReduce.Class
import Control.Arrow.MapReduce.Types

import Control.Input.Class
import Control.Output.Class

import Control.Category
import Control.Cofunctor
import Control.Arrow
import Control.Monad
import Control.Exception
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Concurrent.Chan.Endable

import GHC.Conc

newtype MRParallel input output = MRParallel (forall x . 
  MRInput (x, input)
  -> MROutput (x, output)
  -> IO ())

instance Category MRParallel where
  id = MRParallel $ \ input output -> mapInputM_ (emit output) input
  MRParallel f . MRParallel g = MRParallel $ \ input output -> do
    (gOut, fIn) <- newPipe
    forkIO (g input gOut)
    f fIn output

wrap :: (a -> b) -> (c -> d) -> MRParallel b c -> MRParallel a d
wrap f g (MRParallel run) = MRParallel $ \ input output ->
  run (fmap (fmap f) input) (cofmap (fmap g) output)

instance Arrow MRParallel where
  arr f = MRParallel $ \ input output -> void $ do
    let nThreads = numCapabilities
    test <- newEmptyMVar
    replicateM_ nThreads $ forkIO $ do
      mapInputM_ (\ (x, a) -> do
	b <- evaluate (f a)
	emit output (x, b)) input
      putMVar test () -- this thread is done
    forkIO $ do
      replicateM_ nThreads (takeMVar test) -- wait for everyone to finish
      reportEnd output
  first (MRParallel run) = MRParallel $ \ input output ->
    let runInput = fmap (\ (x, (a, b)) -> ((x, b), a)) input
	runOutput = cofmap (\ ((x, b), a) -> (x, (a, b))) output
    in run runInput runOutput
  second (MRParallel run) = MRParallel $ \ input output ->
    let runInput = fmap (\ (x, (a, b)) -> ((x, a), b)) input
	runOutput = cofmap (\ ((x, a), b) -> (x, (a, b))) output
    in run runInput runOutput