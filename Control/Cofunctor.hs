module Control.Cofunctor where

class Cofunctor f where
  cofmap ::  (b -> a) -> f a -> f b
