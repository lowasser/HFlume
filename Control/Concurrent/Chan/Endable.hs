{-# LANGUAGE RecordWildCards, DoAndIfThenElse, NamedFieldPuns #-}
{-# OPTIONS -funbox-strict-fields #-}
module Control.Concurrent.Chan.Endable (Chan, newChan, writeChan, readChan, terminateChan, isEmptyChan) where

import qualified Control.Concurrent.Chan as C
import Control.Concurrent.MVar
import Control.Monad
import Control.Exception

data Chan a = EChan 
  {channel :: !(C.Chan (Message a)),
    acceptsInput :: !(MVar Bool),
    hasOutput :: !(MVar Bool)}

data Message a = Msg a | Term

newChan :: IO (Chan a)
newChan = do
  channel <- C.newChan
  acceptsInput <- newMVar True
  hasOutput <- newMVar True
  return EChan{..}

writeChan :: Chan a -> a -> IO Bool
writeChan EChan{..} a = withMVar acceptsInput $ \ isReady -> do
  when isReady (C.writeChan channel (Msg a))
  return isReady

readChan :: Chan a -> IO (Maybe a)
readChan EChan{..} = modifyMVar hasOutput $ \ isReady -> 
  if isReady
  then do
	result <- C.readChan channel
	case result of
	  Msg result -> return (True, Just result)
	  Term -> return (False, Nothing)
  else return (False, Nothing)

terminateChan :: Chan a -> IO ()
terminateChan EChan{..} = modifyMVar_ acceptsInput $ \ isReady -> do
  when isReady (C.writeChan channel Term)
  return False

isEmptyChan :: Chan a -> IO Bool
isEmptyChan EChan{hasOutput} = liftM not (readMVar hasOutput)