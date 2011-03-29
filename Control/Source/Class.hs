{-# LANGUAGE BangPatterns #-}
module Control.Source.Class where

import Control.Concurrent.Chan.Endable
import Control.Monad

class Source f where
  tryGet :: f a -> IO (Maybe a)
  isExhausted :: f a -> IO Bool

instance Source Chan where
  tryGet = readChan
  isExhausted chan = liftM not (isEmptyChan chan)

consumeSource :: Source f => f a -> IO [a]
consumeSource src = do
  i <- tryGet src
  case i of
    Nothing	-> return []
    Just x	-> liftM (x:) (consumeSource src)

consumeSource_ :: Source f => f a -> IO ()
consumeSource_ src = do
  i <- tryGet src
  case i of
    Nothing	-> return ()
    Just _	-> consumeSource_ src

mapSource :: Source f => (a -> b) -> f a -> IO [b]
mapSource f src = mapSourceM (return . f) src

mapSourceM :: Source f => (a -> IO b) -> f a -> IO [b]
mapSourceM f src = do
  i <- tryGet src
  case i of
    Nothing	-> return []
    Just x	-> liftM2 (:) (f x) (mapSourceM f src)

mapSourceM_ :: Source f => (a -> IO b) -> f a -> IO ()
mapSourceM_ f src = do
  i <- tryGet src
  case i of
    Nothing	-> return ()
    Just x	-> f x >> mapSourceM_ f src

foldSource, foldSource' :: Source f => (b -> a -> b) -> b -> f a -> IO b
foldSource f = foldSourceM (\ b a -> return (f b a))
foldSource' f = foldSourceM' (\ b a -> return (f b a))

foldSourceM, foldSourceM' :: Source f => (b -> a -> IO b) -> b -> f a -> IO b
foldSourceM f z src = do
  i <- tryGet src
  case i of
    Nothing	-> return z
    Just a	-> do	z' <- f z a
			foldSourceM f z' src

foldSourceM' f z src = do
  i <- tryGet src
  case i of
    Nothing	-> return z
    Just a	-> do	!z' <- f z a
			foldSourceM f z' src