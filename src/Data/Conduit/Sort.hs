{-# LANGUAGE Rank2Types #-}

-- |
-- This package provides the function to sort arbitrary type of data.
-- The sort order obeys comparator of the type, smallest one appears at head.
-- The type to be sorted with this pacakge must be instantiated for class @Sort@.

module Data.Conduit.Sort (
      -- * The @SortOption@ type
      SortOption (..)
    , defaultOption    -- default for SortOption

      -- * Sorting
    , sort
    ) where

import Control.Monad (forM_)
import Control.Monad.Trans (liftIO)
import Data.Conduit
--import Debug.Trace

import qualified Data.ByteString              as S
import qualified Data.Heap                    as H
import qualified Data.Monoid                  as M
import qualified Data.Serialize               as SR
import qualified Data.Vector                  as V
import qualified Data.Vector.Algorithms.Intro as VAI
import qualified Data.Vector.Generic          as G
import qualified Data.Vector.Generic.Mutable  as GM
import qualified Data.Vector.Mutable          as VM
import qualified System.Directory             as FS
import qualified System.IO                    as IO

-- |
-- Sort options.
data SortOption =
    SO  { buffersize :: Int
        }

-- Private data type that holds information of temporary files.
-- The temporary files are used for storing partial sorted data.
data TemporaryFile =
    TempFile
        { getPath   :: IO.FilePath
        , getHandle :: IO.Handle
        }
    
-- Private data type that holds states during partial sorting.
type SortState a = (V.Vector a, Int)

-- Private data type that holds the head of eache partial sorted data.
--
-- data (Sort a) => HeapItem a とか data HeapItem a = (Sort a) => とかもできるけど
-- dataでクラス制約をかいても関数で個別にクラス制約を書く必要があるので意味がない(すごいH本より)
data HeapItem a =
    Item
        { getValue :: a
        , getLeftover :: S.ByteString
        , getTempFile :: TemporaryFile
        }
    -- ^ A heap item.
    -- The value is a parsed element in the sorted data.
    -- The leftover is a buffer that has not yet been consumed
    -- when previous parse succeeded
    -- The tempfile holds the handle and path of a temporary file
    -- that stores partial sorted data.
       
instance Eq a => Eq (HeapItem a) where
    (Item a1 _ _) == (Item a2 _ _) = a1 == a2
    (Item a1 _ _) /= (Item a2 _ _) = a1 /= a2
    
instance Ord a => Ord (HeapItem a) where
    compare (Item a1 _ _) (Item a2 _ _) = compare a1 a2
    (Item a1 _ _) <  (Item a2 _ _) = a1 <  a2
    (Item a1 _ _) <= (Item a2 _ _) = a1 <= a2
    (Item a1 _ _) >  (Item a2 _ _) = a1 >  a2
    (Item a1 _ _) >= (Item a2 _ _) = a1 >= a2
    max t1@(Item a1 _ _) t2@(Item a2 _ _)
        | a1 >= a2  = t1
        | otherwise = t2
    min t1@(Item a1 _ _) t2@(Item a2 _ _)
        | a1 <= a2  = t1
        | otherwise = t2

-- |
-- The default sort option.
defaultOption :: SortOption
defaultOption = SO { buffersize = 4 * 1024 * 1024 }

-- |
-- Sort arbitrary type of data in a way like the sort command in unix.
-- This function splits input into several parts in order to sort huge amount of data.
-- It sorts each part individually and writes into each temporary file.
-- After all input has been sorted with repeating partial sorting,
-- this function merge all temporary files with using heap data structure to create one sorted data.
--
-- >>> runResourceT $ sourceList [8,3,5,6,4] $= sort defaultOption $$ consume
-- [3,4,5,6,8]
--
-- >>> runResourceT $ sourceList ['t','e','s','t'] $= sort defaultOption $$ consume
-- "estt"
sort :: (Ord a, SR.Serialize a, MonadResource m)
     => SortOption
     -> Conduit a m a
sort opt = partialSortWithFiles opt =$= mergeFiles

-- |
-- This function splits input, sorts each part indipendently.
partialSortWithFiles :: (Ord a, SR.Serialize a, MonadResource m)
                     => SortOption 
                     -> Conduit a m TemporaryFile
partialSortWithFiles opt =
    loop initState
  where
    initState :: (Ord a, SR.Serialize a) => SortState a
    initState = (G.create (VM.new $ buffersize opt), 0)
    
    loop :: (Ord a, SR.Serialize a, MonadResource m)
         => SortState a -> Conduit a m TemporaryFile
    loop state = do
        melement <- await
        case melement of
            Nothing -> close state
            Just element -> push state element

    close :: (Ord a, SR.Serialize a, MonadResource m)
          => SortState a -> Conduit a m TemporaryFile
    close (v, size)
        | size <= 0 = M.mempty
        | otherwise = do
            tf <- liftIO $ write $ V.unsafeTake size $ sortv size v        
            yield tf
            M.mempty

    push :: (Ord a, SR.Serialize a, MonadResource m)
         => SortState a -> a -> Conduit a m TemporaryFile
    push (v, size) element
        | newsize < buffersize opt =
            loop (setv size element v, newsize)
        | otherwise = do
            tf <- liftIO $ write $ V.unsafeTake newsize $
                sortv newsize $ setv size element v
            yield tf
            loop initState
      where
        newsize = size + 1
        
    setv :: Int -> a -> V.Vector a -> V.Vector a
    setv size element = V.modify $ \mv ->
        GM.unsafeWrite mv size element
        
    sortv :: (Ord a)
          => Int -> V.Vector a -> V.Vector a
    sortv size = V.modify $ \mv ->
        VAI.sortByBounds compare mv 0 size

    write :: (SR.Serialize a)
          => V.Vector a -> IO TemporaryFile
    write v = do
        (filePath, h) <- IO.openBinaryTempFile "." "_temp_.sort"
        V.mapM_ (S.hPut h . SR.encode) v
        return $ TempFile filePath h

-- |
-- This function merges all temporary files to create one sorted data.
mergeFiles :: (Ord a, SR.Serialize a, MonadResource m)
           => Conduit TemporaryFile m a
mergeFiles =
    takeInput []
  where
    takeInput :: (Ord a, SR.Serialize a, MonadResource m)
              => [TemporaryFile] -> Conduit TemporaryFile m a
    takeInput tfs = do
        mtf <- await
        case mtf of
            Nothing -> terminate tfs
            Just tf -> takeInput (tf : tfs)

    terminate :: (Ord a, SR.Serialize a, MonadResource m)
              => [TemporaryFile] -> Conduit TemporaryFile m a
    terminate tfs = do
        heap <- liftIO $ buildHeap tfs
        yields tfs heap

    buildHeap :: (Ord a, SR.Serialize a) => [TemporaryFile] -> IO (H.MinHeap (HeapItem a))
    buildHeap tfs = do
        mapM_ (\tf -> IO.hSeek (getHandle tf) IO.AbsoluteSeek 0) tfs
        items <- mapM initItem tfs
        return $ foldr build H.empty items
      where
        --initItem :: Sort a => TemporaryFile -> IO (Maybe (HeapItem a))
        initItem tf = getItem tf S.empty
        
        --build :: Sort a => Maybe (HeapItem a) -> H.MinHeap (HeapItem a) -> H.MinHeap (HeapItem a)
        build mitem heap =
            case mitem of
                Nothing   -> heap
                Just item -> H.insert item heap

    yields :: (Ord a, SR.Serialize a, MonadResource m)
           => [TemporaryFile] -> H.MinHeap (HeapItem a) -> Conduit TemporaryFile m a
    yields tfs heap =
        nextheap $ H.view heap
      where
        nextheap Nothing = close tfs
        nextheap (Just (item, heap')) = do
            heap'' <- liftIO $ do
                mitem <- nextItem item
                case mitem of
                  Nothing -> return heap'
                  Just item' -> return $ H.insert item' heap'
            yield $ getValue item
            yields tfs heap''

        --nextItem :: Sort a => HeapItem a -> IO (Maybe (HeapItem a))
        nextItem item = getItem (getTempFile item) (getLeftover item)
    
    -- IO (Maybe a) はMonadとしてのMaybeの特徴を生かすことができないためアンチパターンらしい
    -- ただその理由の大部分はIOErrorをあらわすのにMaybeを使用することの是非からきているようだが
    -- ここでのMaybeは値の存在有無をあらわすので、許される範囲かもしれない
    -- IO (Maybe a) を使わないですますにはデータがないことをあらわすのに例外を使用するとか、
    -- 処理全体が１つのMonadに包まれるようにするなどがあると思う。
    -- 関数内のヘルパーなので、そこまで大掛かりな仕組みを導入するのは不必要 
    getItem :: SR.Serialize a => TemporaryFile -> S.ByteString -> IO (Maybe (HeapItem a))
    getItem tf =
        get (SR.runGetPartial SR.get)
      where
        get :: (S.ByteString -> SR.Result a) -> S.ByteString -> IO (Maybe (HeapItem a))
        get f lo
          | S.null lo = do
              let h = getHandle tf
              eof <- IO.hIsEOF h
              if eof then
                  return Nothing
                else do
                  c <- S.hGet h 4096
                  toItem $ f c
          | otherwise = toItem $ f lo

        toItem (SR.Fail message) = error message -- TODO オプションで変更できるようにする?
        toItem (SR.Done r lo') = return $ Just $ Item r lo' tf 
        toItem (SR.Partial continuation) = get continuation S.empty

    close :: MonadResource m
         => [TemporaryFile] -> Conduit TemporaryFile m a
    close tfs = do
        liftIO $ forM_ tfs $ \tf -> do
            IO.hClose $ getHandle tf
            FS.removeFile $ getPath tf
        M.mempty
