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
  Flatten :: !UniqueId -> [PCollection s a] -> PCollection s a
  GroupByKeyOneShot :: (Eq k, Hashable k) => !UniqueId -> PCollection s (k, a) -> PCollection s (k, OneShot s a)

getPCollID :: PCollection s a -> UniqueId
getPCollID (Explicit i _) = i
getPCollID (Parallel i _ _) = i
getPCollID (Flatten i _) = i
getPCollID (GroupByKeyOneShot i _) = i

instance Eq (PCollection s a) where
  x == y = getPCollID x == getPCollID y

instance Ord (PCollection s a) where
  compare x y = compare (getPCollID x) (getPCollID y)

instance Hashable (PCollection s a) where
  hashWithSalt salt coll = hashWithSalt salt (getPCollID coll)

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

{-# INLINE getPDoID #-}
getPDoID :: PDo s a b -> UniqueId
getPDoID (Map i _) = i
getPDoID (MapMaybe i _) = i
getPDoID (ConcatMap i _) = i
getPDoID _ = undefined

pdoEq :: PDo s a b -> PDo s c d -> Bool
pdoEq Identity Identity = True
pdoEq Identity _ = False
pdoEq _ Identity = False
pdoEq (f :<<: g) (h :<<: k) = pdoEq f h && pdoEq g k
pdoEq (_ :<<: _) _ = False
pdoEq _ (_ :<<: _) = False
pdoEq pdo1 pdo2 = getPDoID pdo1 == getPDoID pdo2

instance Eq (PDo s a b) where
  (==) = pdoEq

instance Hashable (PDo s a b) where
  hashWithSalt salt Identity = salt
  hashWithSalt salt (f :<<: g) = hashWithSalt (hashWithSalt salt f) g
  hashWithSalt salt pdo = hashWithSalt salt (getPDoID pdo)

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