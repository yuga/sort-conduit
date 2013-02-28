{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DefaultSignatures #-}

module Main where

import Control.Monad
import Data.ByteString.Char8 ()
import Data.Conduit
import Data.Conduit.List
import Data.Conduit.Sort
import Test.Hspec
import System.Random

import qualified Data.ByteString as S
import qualified Data.List       as List
import qualified Data.SafeCopy   as SC

type Index = Int
type Value = Int

{-
data Shelf a = Shelf a a

instance (Eq a) => Eq (Shelf a) where
  (Shelf x1 _) == (Shelf x2 _) = x1 == x2 
  (Shelf x1 _) /= (Shelf x2 _) = x1 /= x2

instance (Ord a) => Ord (Shelf a) where
  compare (Shelf x1 _) (Shelf x2 _) = compare x1 x2

instance (Serialize a) => Serialize (Shelf a)
-}

defaultOptionSafe :: (SC.SafeCopy a) => SortOption a
defaultOptionSafe =
    SO { soBufferSize = 4 * 1024 * 1024
       , soGet = SC.safeGet
       , soPut = SC.safePut }

main :: IO ()
main = do
    !r1 <- randomList 100 100
    hspec $ do
        test1
        test2
        test3 r1
    
randomList :: Index -> Value -> IO [Value]
randomList n boundary = replicateM n randomInt
  where
    randomInt :: IO Value
    randomInt = getStdRandom (randomR (0, boundary))

test1 :: Spec
test1 =
    describe "sort short bytestring with default option" $ do
        it "sort an empty string" $
            execSort ["" :: S.ByteString]
                `shouldReturn` [""]
        it "sort a line" $ 
            execSort ["This is a pen" :: S.ByteString]
                 `shouldReturn` ["This is a pen"]
        it "sort multiple lines" $
            execSort ["(3) first", "(1) first", "(2) third" :: S.ByteString]
                 `shouldReturn` ["(1) first", "(2) third", "(3) first"]
      where
        execSort d = runResourceT $ sourceList d $= sort defaultOption $$ consume

test2 :: Spec
test2 =
    describe "sort short string with minimum option" $ do
        it "sort an empty string" $
            execSort ["" :: String]
                `shouldReturn` ["" :: String]
        it "sort a line" $
            execSort ["This is a pen" :: String]
                 `shouldReturn` ["This is a pen" :: String]
        it "sort multiple lines" $
            execSort ["(3) first", "(1) first", "(2) third" :: String]
                 `shouldReturn` ["(1) first", "(2) third", "(3) first" :: String]
      where
        execSort d = runResourceT $ sourceList d $= sort minimumOption $$ consume
        minimumOption = defaultOption { soBufferSize = 10 }

test3 :: [Value] -> Spec
test3 vs =
    describe "sort short int list with minimum option" $ do
        it "sort an empty list" $
            execSort ([]:: [Value])
                `shouldReturn` []
        it "sort a value" $
            execSort ([3] :: [Value])
                 `shouldReturn` [3]
        it "sort multiple values" $
            execSort vs
                 `shouldReturn` List.sort vs
      where
        execSort d = runResourceT $ sourceList d $= sort minimumOption $$ consume
        minimumOption = defaultOption { soBufferSize = 10 }
        
test4 :: [Value] -> Spec
test4 vs =
    describe "sort short int list with minimum option using SafeCopy" $ do
        it "sort an empty list" $
            execSort ([]:: [Value])
                `shouldReturn` []
        it "sort a value" $
            execSort ([3] :: [Value])
                 `shouldReturn` [3]
        it "sort multiple values" $
            execSort vs
                 `shouldReturn` List.sort vs
      where
        execSort d = runResourceT $ sourceList d $= sort minimumOption $$ consume
        minimumOption = defaultOptionSafe { soBufferSize = 10 }
