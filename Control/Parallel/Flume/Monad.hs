{-# LANGUAGE Rank2Types, GADTs #-}
module Control.Parallel.Flume.Monad where

import Control.Parallel.Flume.Types

import Control.Applicative

import Data.Monoid
import Data.Vector (toList)
import qualified Data.HashMap.Strict as HM

newtype Flume s a = Flume {runFlume :: a}

instance Functor (Flume s) where
  fmap f (Flume a) = Flume (f a)

instance Applicative (Flume s) where
  pure = Flume
  Flume f <*> Flume x = Flume (f x)

instance Monad (Flume s) where
  return = Flume
  m >>= k = k (runFlume m)

execFlumeSequential :: (forall s . Flume s (PObject s a)) -> a
execFlumeSequential m = runFlume (seqExecObj (runFlume m))

seqExecObj :: PObject s a -> Flume s a
seqExecObj (Literal a) = return a
seqExecObj (Sequential c) = seqExecColl c
seqExecObj (MapOb f ob) = fmap f (seqExecObj ob)
seqExecObj (Operate f x) = seqExecObj f <*> seqExecObj x
seqExecObj (Concat c) = fmap mconcat (seqExecColl c)

seqExecColl :: PCollection s a -> Flume s [a]
seqExecColl (Parallel pdo c) =
  seqExecPDo pdo (runFlume $ seqExecColl c)
seqExecColl (Explicit xs) = return (toList xs)
seqExecColl (GroupByKeyOneShot ungrouped) = do
  xs <- seqExecColl ungrouped
  let m = foldr (\ (k, a) -> HM.insertWith (\ _ old -> a:old) k [a]) HM.empty xs
  return (HM.toList $ fmap toOneShot m)

seqExecPDo :: PDo s a b -> [a] -> Flume s [b]
seqExecPDo (Map f) xs = return $ fmap f xs
seqExecPDo (MapMaybe f) xs = return $ [y | x <- xs, Just y <- return (f x)]
seqExecPDo (ConcatMap f) xs = return $ concatMap f xs
