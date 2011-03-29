{-# LANGUAGE RecordWildCards, GADTs, KindSignatures #-}
module Control.Arrow.MapReduce.Types where

import Control.Concurrent.Chan.Endable
import Control.Input.Class
import Control.Output.Class
import Control.Cofunctor

data MRInput a where
  WrappedIn :: (b -> a) -> MRInput b -> MRInput a
  SimpleIn :: Chan a -> MRInput a

data MROutput a where
  WrappedOut :: (a -> b) -> MROutput b -> MROutput a
  SimpleOut :: Chan a -> MROutput a

instance Input MRInput where
  tryGet (WrappedIn getMap src) = fmap (fmap getMap) (tryGet src)
  tryGet (SimpleIn ch) = tryGet ch
  isExhausted (WrappedIn _ src) = isExhausted src
  isExhausted (SimpleIn ch) = isExhausted ch

instance Output MROutput where
  emit (WrappedOut putMap dest) a = emit dest (putMap a)
  emit (SimpleOut ch) a = emit ch a
  reportEnd (WrappedOut _ dest) = reportEnd dest
  reportEnd (SimpleOut ch) = reportEnd ch

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