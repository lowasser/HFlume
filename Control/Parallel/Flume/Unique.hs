{-# LANGUAGE MagicHash, UnboxedTuples, GeneralizedNewtypeDeriving #-}
{-# OPTIONS -funbox-strict-fields #-}
module Control.Parallel.Flume.Unique (UniqueId, UniqueT, execUniqueT, newUnique) where

import Control.Monad

import Data.Hashable

import GHC.Exts

newtype UniqueId = Unique Int deriving (Eq, Ord, Hashable)

data StrU a = StrU Int# a
newtype UniqueT m a = UniqueT {runUniqueT :: Int# -> m (StrU a)}

execUniqueT :: Monad m => UniqueT m a -> m a
execUniqueT m = do
  StrU _ a <- runUniqueT m 0#
  return a

instance Monad m => Monad (UniqueT m) where
  {-# INLINE return #-}
  return a = UniqueT $ \ i# -> return (StrU i# a)
  {-# INLINE (>>=) #-}
  m >>= k = UniqueT $ \ i# -> do
    StrU i'# a <- runUniqueT m i#
    runUniqueT (k a) i'#

newUnique :: Monad m => UniqueT m UniqueId
newUnique = UniqueT $ \ i# -> return $ StrU (i# +# 1#) (Unique (I# i#))