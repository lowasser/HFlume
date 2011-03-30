{-# LANGUAGE GADTs #-}
module Control.Arrow.MapReduce.KeySplitter.Linked where

import Control.Sink.Class
import Control.Monad
import Control.Concurrent.MVar

data KeySplitter f a where
  KeySplitter :: Eq k => !(MVar [(k, f (x, a))]) -> (k -> IO (f (x, a))) -> KeySplitter f (x, (k, a))

newKeySplitter :: Eq k => (k -> IO (f (x, a))) -> IO (KeySplitter f (x, (k, a)))
newKeySplitter newKeyMaker = do
  outsVar <- newMVar []
  return (KeySplitter outsVar newKeyMaker)

instance Sink f => Sink (KeySplitter f) where
  emit (KeySplitter outsVar newKey) (x, (k, a)) = modifyMVar_ outsVar $ let
    emitter outs0@((k', kOut):_)
      | k == k'	= emit kOut (x, a) >> return outs0
    emitter (kOut:outs)
      = liftM (kOut:) (emitter outs)
    emitter [] = do
      kOut <- newKey k
      emit kOut (x, a)
      return [(k, kOut)]
    in emitter
  reportEnd (KeySplitter outsVar _) = takeMVar outsVar >>= \ outs -> sequence_ [reportEnd kOut | (_, kOut) <- outs]
   -- we don't replace it