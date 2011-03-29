{-# LANGUAGE TupleSections #-}
module Control.Arrow.MapReduce where

import Data.Hashable
import Control.DeepSeq
import Control.Arrow
import Control.Monad
import Control.Concurrent.Chan.Endable

import Control.Input.Class
import Control.Output.Class

type MRInput a = Chan a
type MROutput a = Chan a

class Arrow a => ArrowMapReduce a where
  mapManyReduce :: (Hashable k, NFData c, NFData d) => 
    (MRInput b -> MROutput (k, c) -> IO ()) -> (k -> MRInput c -> MROutput d -> IO ()) -> a b d

{-# INLINE mapCombine #-}
mapCombine :: (ArrowMapReduce a, NFData c) => (b -> c) -> (c -> c -> c) -> a b c
mapCombine mp (*) = mapManyReduce
  (\ inB outKC -> do
    b1 <- tryGet inB
    case b1 of
      Nothing	-> return ()
      Just b1	-> do	cc <- foldInput' (\ c b -> c * mp b) (mp b1) inB
			emit outKC ((), cc))
  (\ _ inC outD -> do
    c1 <- tryGet inC
    case c1 of
      Nothing	-> return ()
      Just c1	-> foldInput' (*) c1 inC >>= emit outD)

mapMany :: (ArrowMapReduce a, NFData c) => (b -> [c]) -> a b c
mapMany k = mapManyReduce
  (\ inB outKC -> mapInputM_ (\ b -> mapM_ (emit outKC . ((),)) (k b)) inB)
  (\ _ inC outD -> mapInputM_ (emit outD) inC)