{-# LANGUAGE GADTs #-}
module Control.Arrow.MapReduce.Sharder (Sharder(..)) where

import Data.Vector
import Data.Hashable

import Control.Arrow.MapReduce.Types

import Control.Output.Class

import Prelude hiding (length, mapM_)

data Sharder f a where
  Sharder :: Hashable k => !(Vector (f (k, x, a)))
    -> Sharder f (k, x, a)

instance Output f => Output (Sharder f) where
  emit (Sharder shards) x@(k, _, _) = 
    emit (shards ! (abs (hash k) `rem` length shards))
      x
  reportEnd (Sharder shards) = mapM_ reportEnd shards