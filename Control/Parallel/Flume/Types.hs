{-# LANGUAGE GADTs, EmptyDataDecls, TupleSections, Rank2Types #-}
module Control.Parallel.Flume.Types where

import Control.Parallel.Flume.Unique

import Control.Category
import Data.Vector
import Data.Maybe
import Data.Monoid
import Data.Hashable
import qualified Data.List as L
import Prelude hiding ((.), id)

data PCollection s a where
  Explicit :: !UniqueId -> Vector a -> PCollection s a
  Parallel :: !UniqueId -> PDo s a b -> PCollection s a -> PCollection s b
  GroupByKeyOneShot :: (Eq k, Hashable k) => !UniqueId -> PCollection s (k, a) -> PCollection s (k, OneShot s a)

data OneShot s a = OneShot {runOneShot :: forall b . (b -> a -> b) -> b -> b}

toOneShot :: [a] -> OneShot s a
toOneShot xs = OneShot (\ f z -> L.foldl f z xs)

data PDo s a b where
  Identity :: PDo s a a
  Map :: !UniqueId -> (a -> b) -> PDo s a b
  MapMaybe :: !UniqueId -> (a -> Maybe b) -> PDo s a b
  ConcatMap :: !UniqueId -> (a -> [b]) -> PDo s a b
  (:<<:) :: PDo s b c -> PDo s a b -> PDo s a c {- the right operation is never a sequence -}
  -- TODO: side inputs

instance Category (PDo s) where
  id = Identity
  Identity . g = g
  f . Identity = f
  f . (g :<<: h) = (f . g) :<<: h
  f . g = f :<<: g

data PObject s a where
  Operate :: !UniqueId -> PObject s (a -> b) -> PObject s a -> PObject s b
  MapOb :: !UniqueId -> (a -> b) -> PObject s a -> PObject s b
  Sequential :: !UniqueId -> PCollection s a -> PObject s [a]
  Concat :: Monoid a => !UniqueId -> PCollection s a -> PObject s a
  Literal :: !UniqueId -> a -> PObject s a