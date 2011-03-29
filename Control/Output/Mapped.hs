module Control.Output.Mapped where

import Control.Output.Class

data MappedOutput o a b = MappedOutput (o a) (b -> a)

instance Output o => Output (MappedOutput o a) where
  emit (MappedOutput dest f) a = emit dest (f a)
  reportEnd (MappedOutput dest _) = reportEnd dest