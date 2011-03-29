module Control.Output.Fan where

import Control.Concurrent
import Control.Monad

data Fan o a = Fan (o a) !(MVar ()) !(MVar Bool)

instance Output o => Output (Fan o) where
  emit (Fan dest sem term) a = withMVar term $ \ isTerm -> unless isTerm (emit dest a)
  reportEnd (Fan _ sem term) = modifyMVar_ term $ \ isTerm -> do
    unless isTerm (putMVar sem ())
    return True

fanOutputN :: Output o => Int -> o a -> IO [Fan o a]
fanOutputN n dest = do
  !sem <- newEmptyMVar
  forkIO $ do
    replicateM_ n (takeMVar sem)
    reportEnd dest
  replicateM n (liftM (Fan dest sem) (newMVar False))

fanOutput :: Output o => o a -> IO (o a, o a)
fanOutput dest = do
  [out1, out2] <- fanOutputN 2 dest
  return (out1, out2)