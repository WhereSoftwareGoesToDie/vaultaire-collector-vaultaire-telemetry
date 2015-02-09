{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Vaultaire.Collector.Common.Process
import           Vaultaire.Collector.Common.Types

import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Trans
import           Data.Bifunctor
import qualified Data.ByteString.Char8              as BSC (pack)
import qualified Data.HashMap.Strict                as H (fromList)
import qualified Data.Text                          as T (pack)
import           Data.Word                          (Word64)
import           Network.URI
import           Options.Applicative
import           System.ZMQ4                        hiding (shutdown)

import           Marquise.Client                    (hashIdentifier)
import           Vaultaire.Types

helpfulParser :: ParserInfo String
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser String
optionsParser = parseBroker
  where
    parseBroker = strOption $
           long "broker"
        <> short 'b'
        <> metavar "BROKER"
        <> value "tcp://localhost:6660"
        <> showDefault
        <> help "Vault broker URI"


--                  optionsParser initExtraState             cleanup              collect
-- runCollector     Parser o      (CollectorOpts o -> m s)   Collector o s m ()   Collector o s m a

main :: IO ()
main = runCollector optionsParser initialiseExtraState cleanup collect

initialiseExtraState :: CollectorOpts String -> IO (Context,Socket)
initialiseExtraState (_,broker) = do
    c <- context
    sock <- socket c Sub
    connect sock broker
    subscribe sock ""
    return (c,sock)

cleanup :: Collector String (Context,Socket) IO ()
cleanup = return ()



makeCollectableThing :: TeleResp -> Either String (Address, SourceDict, TimeStamp, Word64)
makeCollectableThing TeleResp{..} =
    let TeleMsg{..} = _msg
        sdPairs   = [ ("agent_id",           show _aid)
                    , ("origin",             show _origin)
                    , ("telemetry_msg_type", show _type) ]
        addr      = hashIdentifier $ BSC.pack $ concatMap snd sdPairs
    in do
        sd <- makeSourceDict $ H.fromList $ map (bimap T.pack T.pack) sdPairs
        return (addr, sd, _timestamp, _payload)


collect :: Collector String (Context,Socket) IO ()
collect = do
    (_, (_, sock)) <- get
    forever $ do
        datum <- receive sock
        case (fromWire datum :: Either SomeException TeleResp) of
          Right x -> either (liftIO . putStrLn) collectData (makeCollectableThing x)
          Left  e -> liftIO $ print e


collectData :: (Address, SourceDict, TimeStamp, Word64) -> Collector o s IO ()
collectData (addr, sd, ts, p) = do
    collectSource addr sd
    collectSimple (SimplePoint addr ts p)
