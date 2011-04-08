{-# LANGUAGE BangPatterns #-}
module Control.Parallel.Flume.PCollection (
  fromList,
  fromVector,
  flatten,
  map,
  filter,
  mapMaybe,
  mapMany) where

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

filter :: (a -> Bool) -> PCollection s a -> Flume s (PCollection s a)
filter p = mapMaybe (\ a -> if p a then Just a else Nothing)

mapMaybe :: (a -> Maybe b) -> PCollection s a -> Flume s (PCollection s b)
mapMaybe f = mkDoFn (`MapMaybe` f)

mapMany :: (a -> [b]) -> PCollection s a -> Flume s (PCollection s b)
mapMany f = mkDoFn (`ConcatMap` f)