{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString.Char8 ()
import Data.Conduit
import Data.Conduit.List
import Data.Conduit.Sort
import Test.Hspec

import qualified Data.ByteString as S

main :: IO ()
main = hspec $ do
    test1
    test2

test1 :: Spec
test1 =
    describe "sort short bytestring with default option" $ do
        it "sort an empty string" $ do
            execSort ["" :: S.ByteString]
                `shouldReturn` [""]
        it "sort a line" $ do
            execSort ["This is a pen" :: S.ByteString]
                 `shouldReturn` ["This is a pen"]
        it "sort multiple lines" $ do
            execSort ["(3) first", "(1) first", "(2) third" :: S.ByteString]
                 `shouldReturn` ["(1) first", "(2) third", "(3) first"]
      where
        execSort d = runResourceT $ sourceList d $= sort defaultOption $$ consume

test2 :: Spec
test2 =
    describe "sort short bytestring with minimum option" $ do
        it "sort an empty string" $ do
            execSort ["" :: S.ByteString]
                `shouldReturn` [""]
        it "sort a line" $ do
            execSort ["This is a pen" :: S.ByteString]
                 `shouldReturn` ["This is a pen"]
        it "sort multiple lines" $ do
            execSort ["(3) first", "(1) first", "(2) third" :: S.ByteString]
                 `shouldReturn` ["(1) first", "(2) third", "(3) first"]
      where
        execSort d = runResourceT $ sourceList d $= sort minimumOption $$ consume
        minimumOption = SO 1     
