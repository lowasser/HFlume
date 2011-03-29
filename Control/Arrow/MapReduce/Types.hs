{-# LANGUAGE RecordWildCards, GADTs, KindSignatures #-}
module Control.Arrow.MapReduce.Types where

import Control.Concurrent.Chan.Endable
import Control.Input.Class
import Control.Output.Class

type MRInput a = WrappedChan a
type MROutput a = WrappedChan a

data WrappedChan a where
  WrappedChan ::
    {putMap :: a -> b,
     getMap :: b -> a,
     channel :: Chan b} -> WrappedChan a
  SimpleChan :: Chan a -> WrappedChan a

instance Input WrappedChan where
  tryGet WrappedChan{..} = fmap (fmap getMap) (tryGet channel)
  tryGet (SimpleChan ch) = tryGet ch
  isExhausted WrappedChan{..} = isExhausted channel
  isExhausted (SimpleChan ch) = isExhausted ch

instance Output WrappedChan where
  emit WrappedChan{..} a = emit channel (putMap a)
  emit (SimpleChan ch) a = emit ch a
  reportEnd WrappedChan{..} = reportEnd channel
  reportEnd (SimpleChan ch) = reportEnd ch

newInput :: IO (MRInput a)
newInput = fmap mrInputChan newChan

newOutput :: IO (MROutput a)
newOutput = fmap mrOutputChan newChan

mrInputChan :: Chan a -> MRInput a
mrInputChan = SimpleChan

mrOutputChan :: Chan a -> MROutput a
mrOutputChan = SimpleChan

wrap :: (a -> b) -> (b -> a) -> WrappedChan b -> WrappedChan a
wrap putMap getMap (SimpleChan channel) = WrappedChan{..}
wrap f g WrappedChan{..} = WrappedChan {putMap = putMap . f, getMap = g . getMap, ..}