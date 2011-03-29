{-# LANGUAGE RecordWildCards, GADTs, KindSignatures #-}
module Control.Arrow.MapReduce.Types where

import Control.Concurrent.Chan.Endable
import Control.Concurrent.MVar
import Control.Input.Class
import Control.Output.Class
import Control.Cofunctor
import Control.Monad

data MRInput a where
  WrappedIn :: (b -> a) -> MRInput b -> MRInput a
  SimpleIn :: Chan a -> MRInput a

data MROutput a where
  WrappedOut :: (a -> b) -> MROutput b -> MROutput a
  SimpleOut :: Chan a -> MROutput a
  FannedOut :: MROutput a -> !(MVar Bool) -> !(MVar Bool) -> MROutput a

instance Input MRInput where
  tryGet (WrappedIn getMap src) = fmap (fmap getMap) (tryGet src)
  tryGet (SimpleIn ch) = tryGet ch
  isExhausted (WrappedIn _ src) = isExhausted src
  isExhausted (SimpleIn ch) = isExhausted ch

instance Output MROutput where
  emit (WrappedOut putMap dest) a = emit dest (putMap a)
  emit (SimpleOut ch) a = emit ch a
  emit (FannedOut dest _ doneMe) a = withMVar doneMe $ \ isDoneMe ->
    unless isDoneMe (emit dest a)
  reportEnd (WrappedOut _ dest) = reportEnd dest
  reportEnd (SimpleOut ch) = reportEnd ch
  reportEnd (FannedOut dest doneOne doneMe) = modifyMVar_ doneMe $ \ isDoneMe -> do
    unless isDoneMe $ do
      modifyMVar_ doneOne $ \ isDoneOne -> do
	when isDoneOne (reportEnd dest)
	return True
    return True

fanOutput :: MROutput a -> IO (MROutput a, MROutput a)
fanOutput dest = do
  doneEither <- newMVar False
  done1 <- newMVar False
  done2 <- newMVar False
  return (FannedOut dest doneEither done1, FannedOut dest doneEither done2)

newInput :: IO (MRInput a)
newInput = fmap SimpleIn newChan

newOutput :: IO (MROutput a)
newOutput = fmap SimpleOut newChan

instance Functor MRInput where
  fmap = WrappedIn

instance Cofunctor MROutput where
  cofmap = WrappedOut

newPipe :: IO (MROutput a, MRInput a)
newPipe = do
  ch <- newChan
  return (SimpleOut ch, SimpleIn ch)
