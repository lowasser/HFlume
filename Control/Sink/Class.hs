module Control.Sink.Class where

import Control.Monad

import qualified Control.Concurrent.Chan as C
import qualified Control.Concurrent.Chan.Endable as CE

class Sink f where
  emit :: f a -> a -> IO ()
  reportEnd :: f a -> IO ()

instance Sink C.Chan where
  emit = C.writeChan
  reportEnd _ = return ()

instance Sink CE.Chan where
  emit ch a = void (CE.writeChan ch a)
  reportEnd = CE.terminateChan