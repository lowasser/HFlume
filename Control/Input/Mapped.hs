module Control.Input.Mapped where

import Control.Input.Class
import Control.Monad

data MappedInput i a b = MappedInput (i a) (a -> b)

instance Input i => Input (MappedInput i a) where
  tryGet (MappedInput src f) = liftM (fmap f) (tryGet src)
  isExhausted (MappedInput src _) = isExhausted src