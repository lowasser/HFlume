-- | Waits for there to be no actions left, even while actions are still being added.
module Control.Concurrent.Barrier where

import Control.Concurrent.MVar
import Control.Monad

newtype Barrier = Barrier (MVar [IO ()])

newBarrier :: IO Barrier
newBarrier = liftM Barrier (newMVar [])

addBarrier :: Barrier -> IO () -> IO ()
addBarrier (Barrier waitsVar) wait = modifyMVar_ waitsVar (\ waits -> return (wait:waits))

waitForBarrier :: Barrier -> IO ()
waitForBarrier b@(Barrier waitsVar) = do
  waits <- takeMVar waitsVar
  case waits of
    []	-> putMVar waitsVar []
    (wait:waits) -> do
      putMVar waitsVar waits
      wait
      waitForBarrier b