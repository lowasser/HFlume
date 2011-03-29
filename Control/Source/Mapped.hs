module Control.Source.Mapped where

import Control.Source.Class
import Control.Monad

data MappedSource i a b = MappedSource (i a) (a -> b)

instance Source i => Source (MappedSource i a) where
  tryGet (MappedSource src f) = liftM (fmap f) (tryGet src)
  isExhausted (MappedSource src _) = isExhausted src