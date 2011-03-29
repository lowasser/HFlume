{-# LANGUAGE BangPatterns #-}
module Control.Input.Class where

import Control.Concurrent.Chan.Endable
import Control.Monad

class Input f where
  tryGet :: f a -> IO (Maybe a)
  isExhausted :: f a -> IO Bool

instance Input Chan where
  tryGet = readChan
  isExhausted chan = liftM not (isEmptyChan chan)

consumeInput :: Input f => f a -> IO [a]
consumeInput inp = do
  i <- tryGet inp
  case i of
    Nothing	-> return []
    Just x	-> liftM (x:) (consumeInput inp)

consumeInput_ :: Input f => f a -> IO ()
consumeInput_ inp = do
  i <- tryGet inp
  case i of
    Nothing	-> return ()
    Just _	-> consumeInput_ inp

mapInput :: Input f => (a -> b) -> f a -> IO [b]
mapInput f inp = mapInputM (return . f) inp

mapInputM :: Input f => (a -> IO b) -> f a -> IO [b]
mapInputM f inp = do
  i <- tryGet inp
  case i of
    Nothing	-> return []
    Just x	-> liftM2 (:) (f x) (mapInputM f inp)

mapInputM_ :: Input f => (a -> IO b) -> f a -> IO ()
mapInputM_ f inp = do
  i <- tryGet inp
  case i of
    Nothing	-> return ()
    Just x	-> f x >> mapInputM_ f inp

foldInput, foldInput' :: Input f => (b -> a -> b) -> b -> f a -> IO b
foldInput f = foldInputM (\ b a -> return (f b a))
foldInput' f = foldInputM' (\ b a -> return (f b a))

foldInputM, foldInputM' :: Input f => (b -> a -> IO b) -> b -> f a -> IO b
foldInputM f z inp = do
  i <- tryGet inp
  case i of
    Nothing	-> return z
    Just a	-> do	z' <- f z a
			foldInputM f z' inp

foldInputM' f z inp = do
  i <- tryGet inp
  case i of
    Nothing	-> return z
    Just a	-> do	!z' <- f z a
			foldInputM f z' inp