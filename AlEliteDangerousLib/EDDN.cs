using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using AlEliteDangerousLib.Data.EDDB;
using AlEliteDangerousLib.Log;
using Ionic.Zlib;
using NetMQ;
using NetMQ.Sockets;

namespace AlEliteDangerousLib.Data.EDDN;

/// <summary>
/// EDDN feed.
/// </summary>
public class EDDN
{

    /// <summary>
    /// The EDDN URL.
    /// </summary>
    public static string EDDN_URL = "tcp://eddn.edcd.io:9500";

    public static string marketMessage = "{\"$schemaRef\": \"https://eddn.edcd.io/schemas/commodity";

    /// <summary>
    /// Indicates if we should run.
    /// </summary>
    public static bool IsRunning = true;

    /// <summary>
    /// Allow to pause processing.
    /// </summary>
    private bool IsParsing = false;

    /// <summary>
    /// The queue.
    /// </summary>
    private ConcurrentQueue<string> marketQueue = new ConcurrentQueue<string>();

    /// <summary>
    /// For lookups.
    /// </summary>
    private EDDBDataTransformer eddb = new EDDBDataTransformer();

    /// <summary>
    /// The last time since we parsed EDDB data.
    /// This will parse once every 12 hours.
    /// </summary>
    private DateTimeOffset TimeSinceLastParse = DateTimeOffset.UtcNow;

    /// <summary>
    /// Starts the processing feed.
    /// </summary>
    public void StartFeed(bool lightWeight = false)
    {
        ParseEDDB();

        List<Task> tasks = new List<Task>();
        if (!lightWeight)
        {
            tasks.Add(Task.Run(Ingress));
            tasks.Add(Task.Run(ProcessQueue));
            tasks.Add(Task.Run(CheckForStop));
        }
        else
        {
            tasks.Add(Task.Run(LightWeightProcess));
        }
        Task.WaitAll(tasks.ToArray());
    }

    /// <summary>
    /// Parse the EDDB Lookups.
    /// </summary>
    private void ParseEDDB()
    {
        Logger.Debug($"{GetType().Name}.ParseEDDB:-- START");
        try
        {
            IsParsing = true;
            // Build the lookups
            eddb.ParseAndUpdate(false, false);

            IsParsing = false;
            TimeSinceLastParse = DateTimeOffset.UtcNow;
        }
        catch (Exception ex)
        {
            Logger.Error($"{GetType().Name}.ParseEDDB: ERROR: {ex}");
            IsRunning = false;
            throw;
        }
    }

    /// <summary>
    /// Checks for the stop file.
    /// </summary>
    private void CheckForStop()
    {
        Logger.Log($"{GetType().Name}.CheckForStop:-- START");
        while (IsRunning)
        {
            CheckStop();
            Task.Delay(TimeSpan.FromSeconds(1));
        }
        Logger.Log($"{GetType().Name}.CheckForStop:-- DONE");
    }

    /// <summary>
    /// Checks for stop.
    /// </summary>
    private void CheckStop()
    {
        try
        {
            if (File.Exists("./scripts/stop.stop"))
            {
                Logger.Log($"{GetType().Name}.CheckForStop STOP FILE EXISTS.");
                IsRunning = false;
                return;
            }
        }
        catch (Exception ex)
        {
            Logger.Warning($"{GetType().Name}.CheckForStop WARNING: {ex}");
        }
    }

    /// <summary>
    /// Processes the queue.
    /// </summary>
    private void ProcessQueue()
    {
        Logger.Log($"{GetType().Name}.ProcessQueue:-- START");
        while (IsRunning)
        {
            try
            {
                if ((DateTimeOffset.UtcNow - TimeSinceLastParse).TotalHours > 12)
                {
                    ParseEDDB();
                }
                
                while (marketQueue.Count > 0)
                {
                    if (marketQueue.TryDequeue(out var queueMessage))
                    {
                        ProcessQueueMessage(queueMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"{GetType().Name}.ProcessQueue (queue) ERROR: {ex}");
            }
            
            Task.Delay(TimeSpan.FromMilliseconds(1));
        }
        
        Logger.Log($"{GetType().Name}.ProcessQueue:-- DONE");
    }

    /// <summary>
    /// Process the queue message.
    /// </summary>
    private void ProcessQueueMessage(string queueMessage)
    {
        // we will loose a few messages while parsing.
        if (IsParsing)
        {
            Logger.Warning($"{GetType().Name}.ProcessQueueMessage System waiting for parsing to complete.");
            return;
        }
        EDDNCommodityMessage message = JsonSerializer.Deserialize<EDDNCommodityMessage>(queueMessage);
        
        // ignore / early out.
        if (message == null || message.message == null || !message.message.odyssey) return;
        if (string.IsNullOrEmpty(message.message.systemName)) return;
        if (string.IsNullOrEmpty(message.message.stationName)) return;
        if (message.message.commodities == null || message.message.commodities.Count < 1) return;
        
        // Fill using lookups from eddb.
        message.message.eddbSystem = eddb.Systems.FirstOrDefault(x => x.name.Trim().ToLower() == message.message.systemName.Trim().ToLower());
        message.message.eddbStation = eddb.Stations.FirstOrDefault(x =>
        {
            return x.name.Trim().ToLower() == message.message.stationName.Trim().ToLower() &&
                   x.System.name == message.message.systemName;
        });
        
        if (message.message.eddbSystem == null)
        {
            Logger.Warning($"{GetType().Name}.ProcessQueue Commodity system not found: {message.message.systemName}");
            return;
        }
        if (message.message.eddbStation == null)
        {
            Logger.Warning($"{GetType().Name}.ProcessQueue Commodity station not found: {message.message.stationName}");
            return;
        }
        
        // fill the commodities, filter out poo.
        message.message.commodities.RemoveAll(x => (x.stock < 10000 && x.demand < 10000) || (x.buyPrice < 1 && x.sellPrice < 1));
        message.message.commodities.ForEach(commodity =>
        {
            commodity.eddbCommodity = eddb.Commodities.FirstOrDefault(x => x.name.Trim().ToLower() == commodity.name.Trim().ToLower());
        });
        message.message.commodities.RemoveAll(x => x.eddbCommodity == null);
        if (message.message.commodities.Count < 1) return;
        
        if (message != null)
        {
            if (Logger.LogLevel <= LogLevel.Trace)
            {
                Logger.Trace($"Message: {message}");
            }
        }

        WriteTradeRecord(message.message);
    }

    /// <summary>
    /// Writes a trade record.
    /// </summary>
    private void WriteTradeRecord(EDDNMarket messageMessage)
    {
        //"id","station_id","commodity_id","supply","supply_bracket","buy_price","sell_price","demand","demand_bracket","collected_at"
        messageMessage.commodities.ForEach(commodity =>
        {
            try
            {
                DateTimeOffset datetime = DateTimeOffset.MinValue;
                try
                {
                    datetime = DateTimeOffset.ParseExact(messageMessage.timestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
                }
                catch (Exception)
                {
                    datetime = DateTimeOffset.ParseExact(messageMessage.timestamp, "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
                }

                string line = $"0,{messageMessage.eddbStation.id},{commodity.eddbCommodity.id},{commodity.stock},0,{commodity.buyPrice},{commodity.sellPrice},{commodity.demand},0,{datetime.ToUnixTimeSeconds()}";

                int retry = 10;
                Exception exception = null;
                while (retry-- >= 0)
                {
                    try
                    {
                        File.AppendAllText(EDDB.EDDB.TradeFile, $"{line}\r\n");
                        retry = -1;
                        exception = null;
                    }
                    catch (Exception ex)
                    {
                        exception = ex;
                        Task.Delay(TimeSpan.FromSeconds(1));
                    }
                }
                if (exception != null) Logger.Warning($"{GetType().Name}.WriteTradeRecord write retry: {retry}, WARNING: {exception}");

            }
            catch (Exception ex)
            {
                Logger.Warning($"{GetType().Name}.WriteTradeRecord process WARNING: {ex}");
            }
        });

    }

    /// <summary>
    /// Feeds the queue.
    /// </summary>
    private void Ingress()
    {
        Logger.Log($"{GetType().Name}.Ingress:-- START");

        while (IsRunning)
        {
            try
            {
                long cnt = 0L;
                int errcnt = 0;

                UTF8Encoding utf8 = new UTF8Encoding();
                using SubscriberSocket client = new SubscriberSocket();

                client.Options.ReceiveHighWatermark = 1000;
                client.Connect(EDDN_URL);
                client.SubscribeToAnyTopic();

                while (IsRunning)
                {
                    try
                    {
                        byte[] bytes = client.ReceiveFrameBytes();
                        byte[]? uncompressed = ZlibStream.UncompressBuffer(bytes);

                        if (uncompressed != null)
                        {
                            string result = utf8.GetString(uncompressed);
                            if (!result.StartsWith(marketMessage)) continue;
                            marketQueue.Enqueue(result);

                            if (Logger.LogLevel <= LogLevel.Trace)
                            {
                                Logger.Trace("=================MESSAGE START================");
                                Logger.Trace(result);
                            }
                            
                            cnt += 1;
                            if (cnt % 100 == 0)
                            {
                                Logger.Log($"{GetType().Name}.Ingress Processed msg cnt: {cnt}");
                            }
                        }
                    } //try
                    catch (Exception ex)
                    {
                        Logger.Warning($"{GetType().Name}.Ingress (processing) WARNING: {ex}");
                        errcnt += 1;
                        if (errcnt > 50) break;
                        Task.Delay(TimeSpan.FromMilliseconds(500));
                    }

                    Task.Delay(TimeSpan.FromMilliseconds(1));
                } //while

            }
            catch (Exception ex)
            {
                Logger.Error($"{GetType().Name}.Ingress (connection) ERROR: {ex}");
            }

            // Wait 5 seconds in between retries to reconnect...
            Task.Delay(TimeSpan.FromSeconds(5));
        } //while
        
        Logger.Log($"{GetType().Name}.Ingress:-- DONE");
    }

    
    /// <summary>
    /// Process on the spot.
    /// </summary>
    private async Task LightWeightProcess()
    {
        Logger.Log($"{GetType().Name}.LightWeightProcess:-- START");

        while (IsRunning)
        {
            try
            {
                long cnt = 0L;
                int errcnt = 0;

                UTF8Encoding utf8 = new UTF8Encoding();
                using SubscriberSocket client = new SubscriberSocket();

                client.Options.ReceiveHighWatermark = 1000;
                client.Connect(EDDN_URL);
                client.SubscribeToAnyTopic();

                while (IsRunning)
                {
                    try
                    {
                        CheckStop();
                        if ((DateTimeOffset.UtcNow - TimeSinceLastParse).TotalHours > 12)
                        {
                            ParseEDDB();
                        }
                        byte[] bytes = client.ReceiveFrameBytes();
                        byte[]? uncompressed = ZlibStream.UncompressBuffer(bytes);

                        if (uncompressed != null)
                        {
                            string result = utf8.GetString(uncompressed);
                            if (!result.StartsWith(marketMessage)) continue;
                            ProcessQueueMessage(result);

                            if (Logger.LogLevel <= LogLevel.Trace)
                            {
                                Logger.Trace("=================MESSAGE START================");
                                Logger.Trace(result);
                            }
                            
                            cnt += 1;
                            if (cnt % 100 == 0)
                            {
                                Logger.Log($"{GetType().Name}.Ingress Processed msg cnt: {cnt}");
                            }
                        }
                    } //try
                    catch (Exception ex)
                    {
                        Logger.Warning($"{GetType().Name}.Ingress (processing) WARNING: {ex}");
                        errcnt += 1;
                        if (errcnt > 50) break;
                        await Task.Delay(TimeSpan.FromMilliseconds(500));
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(1));
                } //while

            }
            catch (Exception ex)
            {
                Logger.Error($"{GetType().Name}.Ingress (connection) ERROR: {ex}");
            }

            // Wait 5 seconds in between retries to reconnect...
            await Task.Delay(TimeSpan.FromSeconds(5));
        } //while
        
        Logger.Log($"{GetType().Name}.Ingress:-- DONE");
    }
    
}

[Serializable]
public class EDDNCommodityMessage
{
    [JsonPropertyName("$schemaRef")] public string schemaRef { get; set; }
    
    public EDDNMarket message { get; set; } = null;
    
    public override string ToString()
    {
        return $"schemaRef: {schemaRef}, message: {message}";
    }
    
}


[Serializable]
public class EDDNMarket
{
    public EDStation_EDDB eddbStation { get; set; }
    public EDSystem_EDDB eddbSystem { get; set; }
    
    public string stationName { get; set; }
    public string systemName { get; set; }
    public bool odyssey { get; set; }
    public string timestamp { get; set; }
    
    public List<EDDNCommodity> commodities { get; set; } = new List<EDDNCommodity>();
    
    public override string ToString()
    {
        return $"commodities: {commodities.Count}";
    }
}

[Serializable]
public class EDDNCommodity
{
    public EDCommodity_EDDB eddbCommodity { get; set; }
    public string name { get; set; }
    public int buyPrice { get; set; }
    public int demand { get; set; }
    public int sellPrice { get; set; }
    public int stock { get; set; }
    
    

    public override string ToString()
    {
        return $"name: {name}, buyPrice: {buyPrice}, demand: {demand}, sellPrice: {sellPrice}, stock: {stock}";
    }
}