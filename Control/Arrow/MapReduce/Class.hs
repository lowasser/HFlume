{-# LANGUAGE TupleSections #-}
module Control.Arrow.MapReduce.Class (ArrowMapReduce(..)) where

import Data.Hashable (Hashable)
import Control.DeepSeq
import Control.Arrow
import Control.Monad

import Control.Input.Class
import Control.Output.Class

import Control.Arrow.MapReduce.Types

class Arrow a => ArrowMapReduce a where
  mapManyReduce :: (Eq k, Hashable k, NFData c, NFData d) => 
    Int -> (MRInput (x, b) -> MROutput (k, x, c) -> IO ()) -> (k -> MRInput (x, c) -> MROutput (x, d) -> IO ()) -> a b d

{-# INLINE mapFold1 #-}
mapFold1 :: (a -> b) -> (b -> b -> b) -> MRInput a -> IO (Maybe b)
mapFold1 f (*) inp = do
  a1 <- tryGet inp
  case a1 of
    Nothing	-> return Nothing
    Just a1	-> do	bb <- foldInput (\ b a -> b * f a) (f a1) inp
			return (Just bb)

{-# INLINE emitMaybe #-}
emitMaybe :: MROutput a -> Maybe a -> IO ()
emitMaybe out = maybe (return ()) (emit out)

-- {-# INLINE mapCombine #-}
-- mapCombine :: (ArrowMapReduce a, NFData c) => (b -> c) -> (c -> c -> c) -> a b c
-- mapCombine mp (*) = mapManyReduce 1
--   (\ inB outKC -> do
--     cc <- mapFold1 mp (*) inB
--     emitMaybe outKC $ fmap ((),) cc)
--   (\ _ inC outD -> mapFold1 id (*) inC >>= emitMaybe outD)
-- 
-- mapMany :: (ArrowMapReduce a, NFData c) => (b -> [c]) -> a b c
-- mapMany k = mapManyReduce 1
--   (\ inB outKC -> mapInputM_ (\ b -> mapM_ (emit outKC . ((),)) (k b)) inB)
--   (\ _ inC outD -> mapInputM_ (emit outD) inC)
-- 
-- combine :: (ArrowMapReduce a, NFData b) => (b -> b -> b) -> a b b
-- combine (*) = mapManyReduce 1
--   (\ inB outKC -> do
--       bb <- mapFold1 id (*) inB
--       emitMaybe outKC $ fmap ((),) bb)
--   (\ _ inC outD -> mapFold1 id (*) inC >>= emitMaybe outD)