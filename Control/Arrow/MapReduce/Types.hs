{-# LANGUAGE RecordWildCards, GADTs, KindSignatures #-}
module Control.Arrow.MapReduce.Types where

import Control.Concurrent.Chan.Endable
import Control.Source
import Control.Sink
import Control.Cofunctor
import qualified Control.Sink as Sink

data MRSource a where
  MRSource :: Source src => src a -> MRSource a

data MRSink a where
  MRSink :: Sink snk => snk a -> MRSink a

instance Sink MRSink where
  emit (MRSink dst) a = emit dst a
  reportEnd (MRSink dst) = reportEnd dst

instance Source MRSource where
  tryGet (MRSource src) = tryGet src
  isExhausted (MRSource src) = isExhausted src

newPipe :: IO (MRSink a, MRSource a)
newPipe = do
  ch <- newChan
  return (MRSink ch, MRSource ch)

instance Cofunctor MRSink where
  cofmap f (MRSink sink) = MRSink (MappedSink sink f)

instance Functor MRSource where
  fmap f (MRSource src) = MRSource (MappedSource src f)

fan :: MRSink a -> IO (MRSink a, MRSink a)
fan (MRSink dst) = do
  (dst1, dst2) <- fanSink dst
  return (MRSink dst1, MRSink dst2)

fanN :: Int -> MRSink a -> IO [MRSink a]
fanN n (MRSink dst) = do
  dsts <- fanSinkN n dst
  return (map MRSink dsts)

fan' :: MRSink a -> IO (MRSink a)
fan' (MRSink dst) = fmap MRSink (Sink.fan' dst)