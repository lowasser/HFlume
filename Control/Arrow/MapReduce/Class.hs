{-# LANGUAGE TupleSections, Rank2Types, MultiParamTypeClasses, FlexibleContexts #-}
module Control.Arrow.MapReduce.Class (ArrowMapReduce(..), Mapper, Reducer, mapCombine) where

import Data.Hashable (Hashable)
import Control.DeepSeq
import Control.Arrow
import Control.Monad

import Control.Source
import Control.Sink

import Control.Arrow.MapReduce.Types

type Mapper a k b = forall x . MRSource (x, a) -> MRSink (x, (k, b)) -> IO ()
type Reducer k a b = forall x . k -> MRSource (x, a) -> MRSink (x, b) -> IO ()

class Arrow a => ArrowMapReduce a k where
  mapManyReduce :: Int -> Mapper b k c -> Reducer k c d -> a b d

mapFold1 :: (Source src, Sink snk) => (a -> IO b) -> (b -> b -> IO b) -> src (x, a) -> snk (x, b) -> IO ()
mapFold1 f (*) src dest = do
  xa1 <- tryGet src
  case xa1 of
    Nothing	-> return ()
    Just (x, a) -> do	b <- f a
			bb <- foldSourceM' (\ b a -> f a >>= (*) b) b (MappedSource src snd)
			emit dest (x, bb)

mapCombineMapper :: (x -> y) -> (y -> y -> y) -> Mapper x () y
mapCombineMapper f (*) = \ (MRSource src) (MRSink snk) -> mapFold1 (return . f) (\ a b -> return (a * b))
				src (MappedSink snk (\ (x, b) -> (x, ((), b))))

combineReducer :: (a -> a -> a) -> Reducer () a a
combineReducer (*) = \ _ (MRSource src) (MRSink snk) -> mapFold1 return (\ a b -> return (a * b)) src snk

mapCombine :: ArrowMapReduce a () => (x -> y) -> (y -> y -> y) -> a x y
mapCombine f (*) = mapManyReduce 1 (mapCombineMapper f (*)) (combineReducer (*))
