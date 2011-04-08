{-# LANGUAGE Rank2Types, GADTs #-}
module Control.Parallel.Flume.Execution.Sequential where

import Control.Parallel.Flume.Types

import Control.Applicative
import Control.Monad

import Data.Monoid
import Data.Vector (toList)
import qualified Data.HashMap.Strict as HM

execFlumeSequential :: (forall s . Flume s (PObject s a)) -> a
execFlumeSequential m = runFlume (seqExecObj (runFlume m))

seqExecObj :: PObject s a -> Flume s a
seqExecObj (Literal _ a) = return a
seqExecObj (Sequential _ c) = seqExecColl c
seqExecObj (MapOb _ f ob) = fmap f (seqExecObj ob)
seqExecObj (Operate _ f x) = seqExecObj f <*> seqExecObj x
seqExecObj (Concat _ c) = fmap mconcat (seqExecColl c)

seqExecColl :: PCollection s a -> Flume s [a]
seqExecColl (Parallel _ pdo c) =
  seqExecPDo pdo (runFlume $ seqExecColl c)
seqExecColl (Flatten _ cs) = return $ concatMap (runFlume . seqExecColl) cs
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