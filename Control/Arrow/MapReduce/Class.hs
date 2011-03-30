{-# LANGUAGE TupleSections, Rank2Types, MultiParamTypeClasses #-}
module Control.Arrow.MapReduce.Class (ArrowMapReduce(..), Mapper, Reducer) where

import Data.Hashable (Hashable)
import Control.DeepSeq
import Control.Arrow
import Control.Monad

import Control.Source.Class
import Control.Sink.Class

import Control.Arrow.MapReduce.Types

type Mapper a k b = forall x . MRSource (x, a) -> MRSink (x, (k, b)) -> IO ()
type Reducer k a b = forall x . k -> MRSource (x, a) -> MRSink (x, b) -> IO ()

class Arrow a => ArrowMapReduce a k where
  mapManyReduce :: (NFData c, NFData d) => Int -> Mapper b k c -> Reducer k c d -> a b d