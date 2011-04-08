module Control.Parallel.Flume.PTable where

import Control.Parallel.Flume.Types
import Control.Parallel.Flume.PCollection

import Control.Monad

import Data.Hashable

import Prelude hiding (map)

count :: (Eq a, Hashable a) => PCollection s a -> Flume s (PTable s a Int)
count = map (\ a -> (a, 1)) >=> groupByKeyOneShot >=> combineValues (+)

groupByKeyOneShot :: (Eq k, Hashable k) => PTable s k a -> Flume s (PTable s k (OneShot s a))
groupByKeyOneShot table = do
  groupId <- newID
  return (GroupByKey groupId table)

groupByKey :: (Eq k, Hashable k) => PTable s k a -> Flume s (PTable s k [a])
groupByKey = groupByKeyOneShot >=> mapValues collectOneShot

combineValuesWithKey :: (Eq k, Hashable k) => (k -> a -> a -> a) -> PTable s k (OneShot s a) -> Flume s (PTable s k a)
combineValuesWithKey comb coll = do
  doFnId <- newID
  let doFn = CombineValues doFnId comb
  combinedId <- newID
  return (Parallel combinedId doFn coll)

combineValues :: (Eq k, Hashable k) => (a -> a -> a) -> PTable s k (OneShot s a) -> Flume s (PTable s k a)
combineValues (*) = combineValuesWithKey (const (*))