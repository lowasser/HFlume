module Data.Hashable where

import Data.Int

class Hashable a where
  hash :: a -> Int32

instance Hashable () where
  hash _ = 0