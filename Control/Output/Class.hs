module Control.Output.Class where

import Control.Monad

import qualified Control.Concurrent.Chan as C
import qualified Control.Concurrent.Chan.Endable as CE

class Output f where
  emit :: f a -> a -> IO ()

instance Output C.Chan where
  emit = C.writeChan

instance Output CE.Chan where
  emit ch a = void (CE.writeChan ch a)