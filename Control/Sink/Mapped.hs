module Control.Sink.Mapped where

import Control.Sink.Class

data MappedSink o a b = MappedSink (o a) (b -> a)

instance Sink o => Sink (MappedSink o a) where
  emit (MappedSink dest f) a = emit dest (f a)
  reportEnd (MappedSink dest _) = reportEnd dest