{-# LANGUAGE Arrows #-}
module Control.Arrow.MapReduce.Parallel.Tests (main) where

import Control.Arrow
import Data.List

import Test.QuickCheck
import Test.QuickCheck.Monadic

import Control.Arrow.MapReduce.Parallel
import Control.Arrow.MapReduce.Class

equalsUnordered :: (Show a, Eq a) => [a] -> [a] -> Property
equalsUnordered xs ys = printTestCase ("Actual " ++ show xs ++ " /= Expected" ++ show ys) $ length xs == length ys && null (xs \\ ys)

testComposition :: Property
testComposition = printTestCase "composition" $ \ xs -> monadicIO $ do
  let arrow = proc x -> do
	  x' <- arr (+1) -< x
	  y <- arr (+2) -< x'
	  let z = x' + y
	  returnA -< z
  ys <- run (runMRParallel arrow xs)
  stop (equalsUnordered ys [(x + 1) + ((x + 1) + 2 :: Integer) | x <- xs])

testSide :: Property
testSide = printTestCase "side" $ \ xs -> monadicIO $ do
  let arrow = proc x -> do
	  x' <- arr (+1) -< x
	  y <- arr (+2) -< x
	  let z = x' + y
	  returnA -< z
  ys <- run (runMRParallel arrow xs)
  stop (equalsUnordered ys [(x + 1) + (x + 2 :: Integer) | x <- xs])

tests :: Property
tests = conjoin [testSide, testComposition]

main :: IO ()
main = quickCheck tests