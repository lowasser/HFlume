{-# LANGUAGE GADTs #-}
module Control.Arrow.MapReduce.Sharder (Sharder(..)) where

import Data.Vector
import Data.Hashable

import Control.Arrow.MapReduce.Types

import Control.Sink.Class

import Prelude hiding (length, mapM_)

data Sharder f a where
  Sharder :: Hashable k => !(Vector (f (x, (k, a))))
    -> Sharder f (x, (k, a))

instance Sink f => Sink (Sharder f) where
  emit (Sharder shards) x@(_, (k, _)) = 
    emit (shards ! (abs (hash k) `rem` length shards))
      x
  reportEnd (Sharder shards) = mapM_ reportEnd shards