{-# LANGUAGE BangPatterns #-}
{-# OPTIONS -funbox-strict-fields #-}
module Control.Sink.Fan where

import Control.Concurrent
import Control.Monad

import Control.Sink.Class

data Fan o a = Fan (o a) !(MVar ()) !(MVar Bool)
data Fan' o a = Fan' (o a) !(MVar Bool)

instance Sink o => Sink (Fan o) where
  emit (Fan dest _ term) a = withMVar term $ \ isTerm -> unless isTerm (emit dest a)
  reportEnd (Fan _ sem term) = modifyMVar_ term $ \ isTerm -> do
    unless isTerm (putMVar sem ())
    return True

instance Sink o => Sink (Fan' o) where
  emit (Fan' dest term) a = withMVar term $ \ isTerm -> unless isTerm (emit dest a)
  reportEnd (Fan' _ term) = modifyMVar_ term $ \ _ -> return True

fanSinkN :: Sink o => Int -> o a -> IO [Fan o a]
fanSinkN n dest = do
  !sem <- newEmptyMVar
  forkIO $ do
    replicateM_ n (takeMVar sem)
    reportEnd dest
  replicateM n (liftM (Fan dest sem) (newMVar False))

fanSink :: Sink o => o a -> IO (Fan o a, Fan o a)
fanSink dest = do
  [out1, out2] <- fanSinkN 2 dest
  return (out1, out2)

fan' :: Sink o => o a -> IO (Fan' o a)
fan' dest = liftM (Fan' dest) (newMVar False)