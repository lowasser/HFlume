{-# LANGUAGE Rank2Types, GADTs, TupleSections #-}
module Control.Parallel.Flume.Execution.Sequential where

import Control.Parallel.Flume.Types

import Control.Applicative
import Control.Monad

import qualified Data.Vector as V
import qualified Data.Maybe as M

import Data.Monoid
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
seqExecColl (Explicit _ xs) = return (V.toList xs)
seqExecColl (GroupByKey _ ungrouped) = do
  xs <- seqExecColl ungrouped
  let m = foldr (\ (k, a) -> HM.insertWith (\ _ old -> a:old) k [a]) HM.empty xs
  return (HM.toList (fmap OneShot m))
seqExecColl (MSCR _ coll mapper reducer) = do
  mappedId <- newID
  doFnId <- newID
  let mappedColl = Parallel mappedId (ConcatMap doFnId (runMapper mapper)) coll
  groupedId <- newID
  let groupedColl = GroupByKey groupedId mappedColl
  finalId <- newID
  doFnId' <- newID
  let finalColl = Parallel finalId (ConcatMap doFnId' (\ (k, xs) -> execReducer reducer k xs)) groupedColl
  seqExecColl finalColl

asDoFn :: PDo s a b -> [a] -> [b]
asDoFn (Map _ f) xs = fmap f xs
asDoFn (DoFn _ f) xs = f xs
asDoFn (MapMaybe _ f) xs = M.mapMaybe f xs
asDoFn (MapMaybeValues _ f) xs = M.mapMaybe (\ (k, a) -> fmap (k,) (f k a)) xs
asDoFn (MapValues _ f) xs = fmap (\ (k, a) -> (k, f k a)) xs
asDoFn (Fold _ f z) xs = [foldl f z xs]
asDoFn (CombineValues _ comb) xs =
  [(k, foldl1 (comb k) ys) | (k, OneShot ys) <- xs]
asDoFn (ConcatMap _ f) xs = concatMap f xs
asDoFn (ConcatMapValues _ f) xs = [(k, y) | (k, x) <- xs, y <- f k x]
asDoFn (f :<<: g) xs = asDoFn f $ asDoFn g xs

seqExecPDo :: PDo s a b -> [a] -> Flume s [b]
seqExecPDo doFn xs = return $ asDoFn doFn xs