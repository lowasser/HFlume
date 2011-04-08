{-# LANGUAGE Rank2Types, GADTs #-}
module Control.Parallel.Flume.Execution.Sequential where

import Control.Parallel.Flume.Types

import Control.Applicative
import Control.Monad

import Data.Monoid
import Data.Vector (toList)
import qualified Data.HashMap.Strict as HM

execFlumeSequential :: (forall s . Flume s (PObject s a)) -> a
execFlumeSequential m = execFlume (m >>= seqExecObj)

seqExecObj :: PObject s a -> Flume s a
seqExecObj obj = case obj of
    Literal _ a -> return a
    Sequential _ c -> seqExecColl c
    MapOb _ f ob -> liftM f (seqExecObj ob)
    Operate _ f x -> seqExecObj f <*> seqExecObj x
    Concat _ c -> liftM mconcat (seqExecColl c)

seqExecColl :: PCollection s a -> Flume s [a]
seqExecColl (Parallel _ pdo c) = do
  xs <- seqExecColl c
  seqExecPDo pdo xs
seqExecColl (Flatten _ cs) = liftM concat $ mapM seqExecColl cs
seqExecColl (Explicit _ xs) = return (toList xs)
seqExecColl (GroupByKeyOneShot _ ungrouped) = do
  xs <- seqExecColl ungrouped
  let m = foldr (\ (k, a) -> HM.insertWith (\ _ old -> a:old) k [a]) HM.empty xs
  return (HM.toList $ fmap toOneShot m)

seqExecPDo :: PDo s a b -> [a] -> Flume s [b]
seqExecPDo Identity xs = return xs
seqExecPDo (Map _ f) xs = return $ fmap f xs
seqExecPDo (MapMaybe _ f) xs = return $ [y | x <- xs, Just y <- return (f x)]
seqExecPDo (ConcatMap _ f) xs = return $ concatMap f xs
seqExecPDo (f :<<: g) xs = (seqExecPDo g >=> seqExecPDo f) xs