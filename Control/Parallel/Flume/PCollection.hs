{-# LANGUAGE BangPatterns #-}
module Control.Parallel.Flume.PCollection (
  fromList,
  fromVector,
  flatten,
  map,
  mapValues,
  mapValuesWithKey,
  filter,
  filterValues,
  filterValuesWithKey,
  mapMaybe,
  mapMaybeValues,
  mapMaybeValuesWithKey,
  mapMany,
  mapManyValues,
  mapManyValuesWithKey) where

import Control.Parallel.Flume.Types
import Control.Parallel.Flume.Unique

import qualified Data.Vector as V

import Prelude hiding (map, filter)

fromList :: [a] -> Flume s (PCollection s a)
fromList xs = fromVector (V.fromList xs)

fromVector :: V.Vector a -> Flume s (PCollection s a)
fromVector !xs = do
  collId <- newID
  return (Explicit collId xs)

flatten :: [PCollection s a] -> Flume s (PCollection s a)
flatten colls = do
  collId <- newID
  return (Flatten collId colls)

parallelDo :: PDo s a b -> PCollection s a -> Flume s (PCollection s b)
parallelDo doFn coll = do
  collId <- newID
  return (Parallel collId doFn coll)

mkDoFn :: (UniqueId -> PDo s a b) -> PCollection s a -> Flume s (PCollection s b)
mkDoFn mkFn coll = do
  doFnId <- newID
  parallelDo (mkFn doFnId) coll

map :: (a -> b) -> PCollection s a -> Flume s (PCollection s b)
map f = mkDoFn (`Map` f)

mapValues :: (a -> b) -> PTable s k a -> Flume s (PTable s k b)
mapValues f = mapValuesWithKey (const f)

mapValuesWithKey :: (k -> a -> b) -> PTable s k a -> Flume s (PTable s k b)
mapValuesWithKey f = mkDoFn (`MapValues` f)

filter :: (a -> Bool) -> PCollection s a -> Flume s (PCollection s a)
filter p = mapMaybe (\ a -> if p a then Just a else Nothing)

filterValues :: (a -> Bool) -> PTable s k a -> Flume s (PTable s k a)
filterValues p = filterValuesWithKey (const p)

filterValuesWithKey :: (k -> a -> Bool) -> PTable s k a -> Flume s (PTable s k a)
filterValuesWithKey p = mapMaybeValuesWithKey (\ k a -> if p k a then Just a else Nothing)

mapMaybe :: (a -> Maybe b) -> PCollection s a -> Flume s (PCollection s b)
mapMaybe f = mkDoFn (`MapMaybe` f)

mapMaybeValuesWithKey :: (k -> a -> Maybe b) -> PTable s k a -> Flume s (PTable s k b)
mapMaybeValuesWithKey f = mkDoFn (`MapMaybeValues` f)

mapMaybeValues :: (a -> Maybe b) -> PTable s k a -> Flume s (PTable s k b)
mapMaybeValues f = mapMaybeValuesWithKey (const f)

mapMany :: (a -> [b]) -> PCollection s a -> Flume s (PCollection s b)
mapMany f = mkDoFn (`ConcatMap` f)

mapManyValuesWithKey :: (k -> a -> [b]) -> PTable s k a -> Flume s (PTable s k b)
mapManyValuesWithKey f = mkDoFn (`ConcatMapValues` f)

mapManyValues :: (a -> [b]) -> PTable s k a -> Flume s (PTable s k b)
mapManyValues f = mapManyValuesWithKey (const f)