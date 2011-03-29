{-# LANGUAGE TupleSections #-}
module Control.Arrow.MapReduce.Class (ArrowMapReduce(..)) where

import Data.Hashable (Hashable)
import Control.DeepSeq
import Control.Arrow
import Control.Monad

import Control.Source.Class
import Control.Sink.Class

import Control.Arrow.MapReduce.Types

class Arrow a => ArrowMapReduce a where
  mapManyReduce :: (Eq k, Hashable k, NFData c, NFData d) => 
    Int -> (MRSource (x, b) -> MRSink (k, x, c) -> IO ()) -> (k -> MRSource (x, c) -> MRSink (x, d) -> IO ()) -> a b d