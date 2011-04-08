{-# LANGUAGE GADTs, EmptyDataDecls, TupleSections, Rank2Types #-}
module Control.Parallel.Flume.Types where

import Control.Category
import Control.Applicative
import Control.Arrow
import Control.Monad
import Data.Vector
import Data.Maybe
import Data.Monoid
import Prelude hiding ((.), id)

data PCollection s a where
  Explicit :: Vector a -> PCollection s a
  Parallel :: PDo s a b -> PCollection s a -> PCollection s b
  GroupByKeyOneShot :: Eq k => PCollection s (k, a) -> PCollection s (k, OneShot s a)

data OneShot s a = OneShot {runOneShot :: forall b . (b -> a -> b) -> b -> b}

data PDo s a b where
  Map :: (a -> b) -> PDo s a b
  MapMaybe :: (a -> Maybe b) -> PDo s a b
  ConcatMap :: (a -> [b]) -> PDo s a b
  -- TODO: side inputs

instance Category (PDo s) where
  id = Map id
  Map f . Map g = Map (f . g)
  Map f . MapMaybe g = MapMaybe (fmap f . g)
  Map f . ConcatMap g = ConcatMap (fmap f . g)
  MapMaybe f . Map g = MapMaybe (f . g)
  MapMaybe f . MapMaybe g = MapMaybe (f <=< g)
  MapMaybe f . ConcatMap g = ConcatMap (\ a -> [c | b <- g a, Just c <- return (f b)])
  ConcatMap f . Map g = ConcatMap (f . g)
  ConcatMap f . MapMaybe g = ConcatMap (f <=< maybeToList . g)
  ConcatMap f . ConcatMap g = ConcatMap (f <=< g)

instance Arrow (PDo s) where
  arr = Map
  first (Map f) = Map (first f)
  first (MapMaybe f) = MapMaybe (runKleisli $ first $ Kleisli f)
  first (ConcatMap f) = ConcatMap (runKleisli $ first $ Kleisli f)
  second (Map f) = Map (second f)
  second (MapMaybe f) = MapMaybe (runKleisli $ second $ Kleisli f)
  second (ConcatMap f) = ConcatMap (runKleisli $ second $ Kleisli f)
  Map f *** Map g = Map (f *** g)
  Map f *** MapMaybe g = MapMaybe (\ (a, b) -> fmap (f a,) (g b))
  Map f *** ConcatMap g = ConcatMap (\ (a, b) -> fmap (f a,) (g b))
  MapMaybe f *** Map g = MapMaybe (\ (a, b) -> fmap (,g b) (f a))
  MapMaybe f *** MapMaybe g = MapMaybe (runKleisli $ Kleisli f *** Kleisli g)
  MapMaybe f *** ConcatMap g = ConcatMap (\ (a, b) -> case f a of
    Nothing	-> []
    Just a'	-> fmap (a',) (g b))
  ConcatMap f *** Map g = ConcatMap (\ (a, b) -> fmap (,g b) (f a))
  ConcatMap f *** MapMaybe g = ConcatMap (\ (a, b) -> case g b of
    Nothing	-> []
    Just b'	-> fmap (,b') (f a))
  ConcatMap f *** ConcatMap g = ConcatMap (runKleisli $ Kleisli f *** Kleisli g)

data PObject s a where
  Operate :: PObject s (a -> b) -> PObject s a -> PObject s b
  MapOb :: (a -> b) -> PObject s a -> PObject s b
  Sequential :: PCollection s a -> PObject s [a]
  Concat :: Monoid a => PCollection s a -> PObject s a
  Literal :: a -> PObject s a

instance Functor (PObject s) where
  fmap f (MapOb g ob) = MapOb (f . g) ob
  fmap f (Literal a) = Literal (f a)
  fmap f ob = MapOb f ob

instance Applicative (PObject s) where
  pure = Literal
  Literal f <*> ob = fmap f ob
  f <*> ob = Operate f ob