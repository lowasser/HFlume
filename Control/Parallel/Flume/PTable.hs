module Control.Parallel.Flume.PTable where

import Control.Parallel.Flume.Types
import Control.Parallel.Flume.PCollection
import Control.Parallel.Flume.Unique

import Control.Monad

import Data.Hashable

import Prelude hiding (map)

parallelDo :: PDo s a b -> PCollection s a -> Flume s (PCollection s b)
parallelDo doFn coll = do
  collId <- newID
  return (Parallel collId doFn coll)

mkDoFn :: (UniqueId -> PDo s a b) -> PCollection s a -> Flume s (PCollection s b)
mkDoFn mkFn coll = do
  doFnId <- newID
  parallelDo (mkFn doFnId) coll

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

mapValues :: (a -> b) -> PTable s k a -> Flume s (PTable s k b)
mapValues f = mapValuesWithKey (const f)

mapValuesWithKey :: (k -> a -> b) -> PTable s k a -> Flume s (PTable s k b)
mapValuesWithKey f = mkDoFn (`MapValues` f)

filterValues :: (a -> Bool) -> PTable s k a -> Flume s (PTable s k a)
filterValues p = filterValuesWithKey (const p)

filterValuesWithKey :: (k -> a -> Bool) -> PTable s k a -> Flume s (PTable s k a)
filterValuesWithKey p = mapMaybeValuesWithKey (\ k a -> if p k a then Just a else Nothing)

mapMaybeValuesWithKey :: (k -> a -> Maybe b) -> PTable s k a -> Flume s (PTable s k b)
mapMaybeValuesWithKey f = mkDoFn (`MapMaybeValues` f)

mapMaybeValues :: (a -> Maybe b) -> PTable s k a -> Flume s (PTable s k b)
mapMaybeValues f = mapMaybeValuesWithKey (const f)

mapManyValues :: (a -> [b]) -> PTable s k a -> Flume s (PTable s k b)
mapManyValues f = mapManyValuesWithKey (const f)

mapManyValuesWithKey :: (k -> a -> [b]) -> PTable s k a -> Flume s (PTable s k b)
mapManyValuesWithKey f = mkDoFn (`ConcatMapValues` f)