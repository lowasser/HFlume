{-# LANGUAGE GADTs, EmptyDataDecls, TupleSections, Rank2Types, GeneralizedNewtypeDeriving #-}
module Control.Parallel.Flume.Types where

import Control.Parallel.Flume.Unique

import Control.Applicative
import Control.Monad

import Data.Functor.Identity (Identity, runIdentity)

import Data.Vector
import Data.Maybe
import Data.Monoid
import Data.Hashable
import Prelude hiding ((.), id)

newtype Flume s a = Flume (UniqueT Identity a) deriving (Monad)

newID :: Flume s UniqueId
newID = Flume newUnique

execFlume :: (forall s . Flume s a) -> a
execFlume (Flume m) = runIdentity $ execUniqueT m

instance Functor (Flume s) where
  fmap = liftM

instance Applicative (Flume s) where
  pure = return
  (<*>) = ap

data PCollection s a where
  Explicit :: !UniqueId -> !(Vector a) -> PCollection s a
  Parallel :: !UniqueId -> PDo s a b -> PCollection s a -> PCollection s b
  Flatten :: !UniqueId -> [PCollection s a] -> PCollection s a
  GroupByKey :: (Eq k, Hashable k) => !UniqueId -> PCollection s (k, a) -> PCollection s (k, OneShot s a)
  MSCR :: (Eq k, Hashable k) =>
    !UniqueId -> PCollection s a -> Mapper s a k b -> Reducer s k b c -> PCollection s c

newtype Mapper s a k b = Mapper {runMapper :: a -> [(k, b)]}
newtype Reducer s k a b = Reducer {runReducer :: k -> [a] -> [b]}

execReducer :: Reducer s k a b -> k -> OneShot s a -> [b]
execReducer m k (OneShot xs) = runReducer m k xs

newtype OneShot s a = OneShot [a]

collectOneShot :: OneShot s a -> [a]
collectOneShot (OneShot xs) = xs

type PTable s k a = PCollection s (k, a)

getPCollID :: PCollection s a -> UniqueId
getPCollID (MSCR i _ _ _) = i
getPCollID (Explicit i _) = i
getPCollID (Parallel i _ _) = i
getPCollID (Flatten i _) = i
getPCollID (GroupByKey i _) = i

instance Eq (PCollection s a) where
  x == y = getPCollID x == getPCollID y

instance Ord (PCollection s a) where
  compare x y = compare (getPCollID x) (getPCollID y)

instance Hashable (PCollection s a) where
  hashWithSalt salt coll = hashWithSalt salt (getPCollID coll)

data PDo s a b where
  Map :: !UniqueId -> (a -> b) -> PDo s a b
  MapValues :: !UniqueId -> (k -> a -> b) -> PDo s (k, a) (k, b)
  MapMaybe :: !UniqueId -> (a -> Maybe b) -> PDo s a b
  MapMaybeValues :: !UniqueId -> (k -> a -> Maybe b) -> PDo s (k, a) (k, b)
  ConcatMap :: !UniqueId -> (a -> [b]) -> PDo s a b
  ConcatMapValues :: !UniqueId -> (k -> a -> [b]) -> PDo s (k, a) (k, b)
  Fold :: !UniqueId -> (b -> a -> b) -> b -> PDo s a b
  CombineValues :: !UniqueId -> (k -> a -> a -> a) -> PDo s (k, OneShot s a) (k, a)
  DoFn :: !UniqueId -> ([a] -> [b]) -> PDo s a b
  (:<<:) :: PDo s b c -> PDo s a b -> PDo s a c {- the right operation is never a sequence -}
  -- TODO: side inputs

{-# INLINE getPDoID #-}
getPDoID :: PDo s a b -> UniqueId
getPDoID (Map i _) = i
getPDoID (MapMaybe i _) = i
getPDoID (ConcatMap i _) = i
getPDoID (DoFn i _) = i
getPDoID (MapValues i _) = i
getPDoID (MapMaybeValues i _) = i
getPDoID (Fold i _ _) = i
getPDoID (CombineValues i _) = i
getPDoID _ = undefined

pdoEq :: PDo s a b -> PDo s c d -> Bool
pdoEq (f :<<: g) (h :<<: k) = pdoEq f h && pdoEq g k
pdoEq (_ :<<: _) _ = False
pdoEq _ (_ :<<: _) = False
pdoEq pdo1 pdo2 = getPDoID pdo1 == getPDoID pdo2

instance Eq (PDo s a b) where
  (==) = pdoEq

instance Hashable (PDo s a b) where
  hashWithSalt salt (f :<<: g) = hashWithSalt (hashWithSalt salt f) g
  hashWithSalt salt pdo = hashWithSalt salt (getPDoID pdo)

data PObject s a where
  Operate :: !UniqueId -> PObject s (a -> b) -> PObject s a -> PObject s b
  MapOb :: !UniqueId -> (a -> b) -> PObject s a -> PObject s b
  Sequential :: !UniqueId -> PCollection s a -> PObject s [a]
  Concat :: Monoid a => !UniqueId -> PCollection s a -> PObject s a
  Literal :: !UniqueId -> a -> PObject s a

getPObjID :: PObject s a -> UniqueId
getPObjID (Operate i _ _) = i
getPObjID (MapOb i _ _) = i
getPObjID (Sequential i _) = i
getPObjID (Concat i _) = i
getPObjID (Literal i _) = i

instance Eq (PObject s a) where
  o1 == o2 = getPObjID o1 == getPObjID o2

instance Hashable (PObject s a) where
  hashWithSalt salt obj = hashWithSalt salt (getPObjID obj)