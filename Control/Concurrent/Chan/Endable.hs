{-# LANGUAGE DeriveDataTypeable #-}
{-# OPTIONS -funbox-strict-fields #-}
module Control.Concurrent.Chan.Endable (Chan, newChan, writeChan, readChan, terminateChan, isEmptyChan) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Exception

import Data.Typeable

data Chan a = Chan !(MVar (Stream a)) !(MVar (WriteEnd a))
data WriteEnd a = StreamEnd | WriteHole !(Stream a)
type Stream a = MVar (ChItem a)
data ChItem a = ChItem a !(Stream a) | ChEnd

data WriteToTerminatedStream = WriteToTerminatedStream deriving (Show, Typeable)

instance Exception WriteToTerminatedStream

newChan :: IO (Chan a)
newChan = do
  hole <- newEmptyMVar
  readVar <- newMVar hole
  writeVar <- newMVar (WriteHole hole)
  return (Chan readVar writeVar)

writeChan :: Chan a -> a -> IO ()
writeChan (Chan _ writeVar) a = modifyMVar_ writeVar $ \ writeEnd -> case writeEnd of
  StreamEnd	-> throwIO WriteToTerminatedStream
  WriteHole old_hole -> do
    new_hole <- newEmptyMVar
    putMVar old_hole (ChItem a new_hole)
    return (WriteHole new_hole)

readChan :: Chan a -> IO (Maybe a)
readChan (Chan readVar _) = modifyMVar readVar $ \ read_end -> do
    next <- takeMVar read_end
    case next of
      ChEnd	-> do
	putMVar read_end ChEnd
	return (read_end, Nothing)
      ChItem a next_read_end -> do
	return (next_read_end, Just a)

terminateChan :: Chan a -> IO ()
terminateChan (Chan _ writeVar) = modifyMVar_ writeVar $ \ writeEnd -> case writeEnd of
  WriteHole old_hole -> do
    putMVar old_hole ChEnd
    return StreamEnd
  StreamEnd -> return StreamEnd

isEmptyChan :: Chan a -> IO Bool
isEmptyChan (Chan readVar _) = withMVar readVar $ \ read_end -> withMVar read_end $ \ next -> case next of
  ChEnd	-> return True
  _	-> return False