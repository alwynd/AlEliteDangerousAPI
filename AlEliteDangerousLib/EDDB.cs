using System.Collections.Concurrent;
using System.Globalization;
using System.Numerics;
using System.Text.Json;
using AlEliteDangerousLib.Log;
using AlEliteDangerousLib.Web;

namespace AlEliteDangerousLib.Data.EDDB
{
    /// <summary>
    /// Responsible for obtaining the EDDB data.
    /// </summary>
    public class EDDB
    {
        /// <summary>
        /// The eddb data folder.
        /// </summary>
        public static string DataFolder = $"{DataUtil.DATA_FOLDER}/EDDB";

        
        /// <summary>
        /// The EDDB URL.
        /// </summary>
        public static string EDDB_URL = "https://eddb.io/archive/v6";
        
        /// <summary>
        /// The trade data url.
        /// </summary>
        public static string TRADE_URL = $"{EDDB_URL}/listings.csv";
        
        /// <summary>
        /// Stations.
        /// </summary>
        public static string STATIONS_URL = $"{EDDB_URL}/stations.json";

        /// <summary>
        /// Systems.
        /// </summary>
        public static string SYSTEMS_URL = $"{EDDB_URL}/systems_populated.json";
        
        /// <summary>
        /// Systems.
        /// </summary>
        public static string COMMODITIES_URL = $"{EDDB_URL}/commodities.json";
        
        
        /// <summary>
        /// The eddb file.
        /// </summary>
        public static string TradeFile = $"{DataFolder}/trade.csv";

        /// <summary>
        /// The eddb file.
        /// </summary>
        public static string StationsFile = $"{DataFolder}/stations.json";

        /// <summary>
        /// The eddb file.
        /// </summary>
        public static string SystemsFile = $"{DataFolder}/systems.json";

        /// <summary>
        /// The eddb file.
        /// </summary>
        public static string CommoditiesFile = $"{DataFolder}/commodities.json";

        /// <summary>
        /// Write the output to CSV.
        /// </summary>
        public static string TradeOutputCSV = $"{DataFolder}/TradeOutput.csv";

        /// <summary>
        /// Write the output to CSV.
        /// </summary>
        public static string ShortJumpTradeOutputCSV = $"{DataFolder}/TradeOutputShortJumps.csv";

        /// <summary>
        /// Write the output to CSV.
        /// </summary>
        public static string TradeMultihopOutputCSV = $"{DataFolder}/TradeOutputMultihop.csv";

        /// <summary>
        /// Write the output to CSV.
        /// </summary>
        public static string TradeHighestOutputCSV = $"{DataFolder}/TradeOutputHighest.csv";

        /// <summary>
        /// Write the data stats.
        /// </summary>
        public static string DataStatsCSV = $"{EDDB.DataFolder}/DataStats.csv";

        
        /// <summary>
        /// Stops all processing due to exception.
        /// </summary>
        public Exception mainException = null;
        
        /// <summary>
        /// The data transformer.
        /// </summary>
        private EDDBDataTransformer transformer = new EDDBDataTransformer();
        
        
        /// <summary>
        /// Checks if there is new data available.
        /// </summary>
        public void Update()
        {
            string msg = $"{GetType().Name}.Update";
            Logger.Debug($"{msg}:-- START");
            try
            {
                DataUtil.Instance.StartTimer(msg);
                List<Task> tasks = new List<Task>();
                tasks.Add(UpdateFile(TRADE_URL, TradeFile));
                tasks.Add(UpdateFile(STATIONS_URL, StationsFile));
                tasks.Add(UpdateFile(SYSTEMS_URL, SystemsFile));
                tasks.Add(UpdateFile(COMMODITIES_URL, CommoditiesFile));

                Task.WaitAll(tasks.ToArray());
                if (mainException != null) throw mainException;
                
                // clean old trades out, to keep this process going.
                tasks = new List<Task>();
                tasks.Add(Task.Run(CleanTradeFile));
                Task.WaitAll(tasks.ToArray());
                if (mainException != null) throw mainException;

                transformer.ParseAndUpdate(true, true);
            } //try
            finally
            {
                Logger.Info($"{msg} - {DataUtil.Instance.StopTimer(msg)}");                
            } //finally

        }

        /// <summary>
        /// Cleans the trade file, of any trades older than 5 days, as they are meaningless.
        /// </summary>
        private void CleanTradeFile()
        {
            Logger.Debug($"{GetType().Name}.CleanTradeFile:-- START file: {EDDB.TradeFile}");

            try
            {
                // read in trade file
                List<string> lines = File.ReadAllLines(EDDB.TradeFile).ToList();
                int original = lines.Count;
                int now = 0;
                int diff = 0;

                Logger.Debug($"{GetType().Name}.CleanTradeFile file: {EDDB.TradeFile}, lines: {original}");

                // remove old rows
                lines.RemoveAll(x =>
                {
                    if (x.StartsWith("id")) return true;    // discard the header.
                    string[] arr = x.Split(",");
                    DateTimeOffset dte = DateTimeOffset.FromUnixTimeSeconds(long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("collected_at")]));
                    return (DateTimeOffset.UtcNow - dte).TotalDays > 5;
                });
                
                // only keep the last entry for a given commodity at the specific station market, as the old data becomes stale.
                // sort all by date desc, discard any by dupl key: station_id_commodity
                Logger.Trace($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} SORT DATE DESC");
                List<string> processlines = new List<string>(lines);
                processlines.Sort((a, b) =>
                {
                    string[] arr = a.Split(",");
                    DateTimeOffset adte = DateTimeOffset.FromUnixTimeSeconds(long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("collected_at")]));

                    string[] brr = b.Split(",");
                    DateTimeOffset bdte = DateTimeOffset.FromUnixTimeSeconds(long.Parse(brr[EDDBDataTransformer.TradeMappings.IndexOf("collected_at")]));

                    return (bdte.CompareTo(adte));
                });

                // show the first and last date + validation.
                {
                    string[] ar1 = processlines[0].Split(",");
                    DateTimeOffset fdte = DateTimeOffset.FromUnixTimeSeconds(long.Parse(ar1[EDDBDataTransformer.TradeMappings.IndexOf("collected_at")]));

                    ar1 = processlines[processlines.Count - 1].Split(",");
                    DateTimeOffset ldte = DateTimeOffset.FromUnixTimeSeconds(long.Parse(ar1[EDDBDataTransformer.TradeMappings.IndexOf("collected_at")]));

                    Logger.Debug($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} SORT DATE DESC...RESULT: first date: {fdte}, last date: {ldte}");
                    if (fdte < ldte) throw new Exception($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} SORT DATE DESC...RESULT: first date: {fdte}, last date: {ldte}: SORT FAILED! date[0] < date[last]");
                }

                {
                    Logger.Debug($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} Discard old commodities.");
                    int pnow = processlines.Count;
                    Dictionary<string, bool> keys = new Dictionary<string, bool>();
                    Dictionary<string, List<TradeCleanupRecord>> cleanup = new Dictionary<string, List<TradeCleanupRecord>>();
                    List<int> removeIndices = new List<int>();
                    int i = 0;
                    
                    // build index
                    Logger.Debug($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} Discard old commodities...Build Index");
                    lines.ForEach(x =>
                    {
                        string[] arr = x.Split(",");
                        long station_id = long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("station_id")]);
                        long commodity_id = long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("commodity_id")]);
                        DateTimeOffset dte = DateTimeOffset.FromUnixTimeSeconds(long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("collected_at")]));
                        string key = $"{station_id}_{commodity_id}";
                        if (!cleanup.ContainsKey(key)) cleanup[key] = new List<TradeCleanupRecord>();
                        
                        cleanup[key].Add(new TradeCleanupRecord()
                        {
                            Station_id = station_id,
                            Commodity_id = commodity_id,
                            Date = dte,
                            LineIndex = i++
                        });
                    });
                    
                    //gather removal indices
                    Logger.Debug($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} Discard old commodities...Gather removal indices..");
                    processlines.ForEach(x =>
                    {
                        string[] arr = x.Split(",");
                        long station_id = long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("station_id")]);
                        long commodity_id = long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("commodity_id")]);
                        DateTimeOffset dte = DateTimeOffset.FromUnixTimeSeconds(long.Parse(arr[EDDBDataTransformer.TradeMappings.IndexOf("collected_at")]));
                        string key = $"{station_id}_{commodity_id}";
                    
                        // dont process stuff you did before. *saves processing time.
                        if (!keys.ContainsKey(key))
                        {
                            keys[key] = true;
                            removeIndices.AddRange(cleanup[key].Where(x => x.Date < dte).Select(x => x.LineIndex));
                        }
                    });
                    
                    Logger.Debug($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} Discard old commodities...Reverse sort and remove by index.");
                    //reverse sort & remove ( then indices will remain in tact as reverse removal )
                    removeIndices.Sort((a, b) => b.CompareTo(a));    
                    removeIndices.ForEach(x => lines.RemoveAt(x));
                    
                    Logger.Info($"{GetType().Name}.CleanTradeFile, file: {EDDB.TradeFile} Discard old commodities....Prev: {pnow}, Now: {lines.Count}, Diff: {pnow - lines.Count}");
                }

                now = lines.Count;
                diff = original - now;

                if (diff == 0)
                {
                    Logger.Info($"{GetType().Name}.CleanTradeFile:-- DONE (NO DIFFERENCE) file: {EDDB.TradeFile}, prev: {original}, now: {now}, Diff: {diff}");
                    return;
                }

                // write out temp file
                File.WriteAllLines(EDDB.TradeFile + ".tmp", lines);

                // delete and rename
                int retry = 5;
                while (retry-- >= 0)
                {
                    try
                    {
                        File.Delete(EDDB.TradeFile);
                        retry = -1;
                    }
                    catch (Exception ex)
                    {
                        Logger.Warning($"{GetType().Name}.CleanTradeFile WARNING (delete) retry: {retry}, file: {EDDB.TradeFile}, {ex}");
                        Task.Delay(TimeSpan.FromSeconds(1));
                    }
                }

                if (File.Exists(EDDB.TradeFile)) throw new Exception("Unable to delete the trade file.");

                // rename
                retry = 5;
                while (retry-- >= 0)
                {
                    try
                    {
                        File.Move(EDDB.TradeFile + ".tmp", EDDB.TradeFile);
                        retry = -1;
                    }
                    catch (Exception ex)
                    {
                        Logger.Warning($"{GetType().Name}.CleanTradeFile WARNING (move) retry: {retry}, file: {EDDB.TradeFile}.tmp, {ex}");
                        Task.Delay(TimeSpan.FromSeconds(1));
                    }
                }

                if (!File.Exists(EDDB.TradeFile)) throw new Exception("Unable to move the trade file.");
                Logger.Info($"{GetType().Name}.CleanTradeFile:-- DONE file: {EDDB.TradeFile}, prev: {original}, now: {now}, Diff: {diff}");

            }
            catch (Exception ex)
            {
                mainException = ex;
                throw;
            }
        }

        /// <summary>
        /// Downloads files, if need be.
        /// </summary>
        private async Task UpdateFile(string url, string file, int hours = 12)
        {
            string msg = $"{GetType().Name}.UpdateFile file: {file}";
            string result = "NOT OK";
            string sz = "N/A";
            try
            {
                DataUtil.Instance.StartTimer(msg);
                Logger.Debug($"{msg}:-- START, url: {url}");
                // don't update if the file is not older than 12 hours.
                if (!DataUtil.Instance.IsOlderThanHours(file, hours))
                {
                    Logger.Debug($"{GetType().Name}.UpdateFile url: {url}, DataFolder: {file} last updated within 12 hours...ignoring request..");
                    return;
                }

                // headers first, in case the server is down.
                Dictionary<string, List<string>> headers = await WebUtil.HttpHead(url);

                // TODO first download to temp, then delete/copy
                await WebUtil.HttpDownload(url, file);
                if (!File.Exists(file)) throw new Exception($"{GetType().Name}.UpdateFile Could not download file. ( file not exists ) url: {url}");

                // assert it worked
                FileInfo info = new FileInfo(file);
                if (info.Length < 1L) throw new Exception($"{GetType().Name}.UpdateFile Could not download file. ( 0 length ) url: {url}");

                result = "OK";
                sz = DataUtil.FormatSize(info.Length);
                // log
            } // try
            finally
            {
                Logger.Info($"{msg} url: {url}, ...{result} size: {sz} - {DataUtil.Instance.StopTimer(msg)}");
            }
        }


        /// <summary>
        /// Clean the data folder.
        /// </summary>
        public void Clean()
        {
            string msg = $"{GetType().Name}.Clean";
            DataUtil.Instance.StartTimer(msg);
            
            Logger.Debug($"{msg}:-- START, DataFolder: {DataFolder}");
            Directory.Delete(DataFolder, true);
            
            Logger.Info($"{msg} - {DataUtil.Instance.StopTimer(msg)}");
        }
    }


    /// <summary>
    /// Normalize and map data. 
    /// </summary>
    public class EDDBDataTransformer
    {
        /// <summary>
        /// The trade output CSV HEADER.
        /// </summary>
        public static string TRADE_CSV_HEADER = "Commodity,Buy System,Buy Station,Buy Distance,Buy Price,Supply,Buy Age,Sell System,Sell Station,Sell Distance,Sell Price,Demand,Sell Age,Jump Distance,ReturnTrip,Profit,TotalProfit,+Cargo $Mil";
        
        /// <summary>
        /// The MinimumDemand.
        /// </summary>
        public static int MinimumDemand { get; set; } = 20000;

        /// <summary>
        /// The MinimumLandingPadSize.
        /// </summary>
        public static string MinimumLandingPadSize { get; set; } = "L";

        /// <summary>
        /// The Maximum trade data age.
        /// </summary>
        public static int MaximumTradeDataAgeInHours { get; set; } = 12;

        /// <summary>
        /// The minimum profit.
        /// </summary>
        public static int MinimumProfit { get; set; } = 19000;

        /// <summary>
        /// The maximum distance between systems to trade.
        /// </summary>
        public static int MaximumDistanceBetweenTradeSystems { get; set; } = 50;

        /// <summary>
        /// Maximum distance from star.
        /// </summary>
        public static int MaximumDistanceFromStar { get; set; } = 500;

        /// <summary>
        /// The minimum cargo space.
        /// </summary>
        public static int MaximumCargoSpace { get; set; } = 720;

        
        /// <summary>
        /// The EDDB systems.
        /// </summary>
        public List<EDSystem_EDDB> Systems = new List<EDSystem_EDDB>();
        
        /// <summary>
        /// The EDDB stations.
        /// </summary>
        public List<EDStation_EDDB> Stations = new List<EDStation_EDDB>();
        
        
        /// <summary>
        /// The EDDB Commodities.
        /// </summary>
        public List<EDCommodity_EDDB> Commodities = new List<EDCommodity_EDDB>();
        
        /// <summary>
        /// The EDDB trades ( only recent are kept )
        /// </summary>
        public List<EDTrade_EDDB> Trades = new List<EDTrade_EDDB>();

        /// <summary>
        /// Trade CSV column to index mappings.
        /// </summary>
        public static List<string> TradeMappings = new List<string>() {"id","station_id","commodity_id","supply","supply_bracket","buy_price","sell_price","demand","demand_bracket","collected_at"};

        /// <summary>
        /// The DataStats CSV Header.
        /// </summary>
        public static string DataStatsCSVHeader = "Systems,Stations,Commodities,All Station Commodities,Timestamp";
        
        
        /// <summary>
        /// For Indexing.
        /// </summary>
        public ConcurrentDictionary<long, EDSystem_EDDB> systemIndex = new ConcurrentDictionary<long, EDSystem_EDDB>();

        /// <summary>
        /// For Indexing.
        /// </summary>
        public ConcurrentDictionary<long, EDStation_EDDB> stationIndex = new ConcurrentDictionary<long, EDStation_EDDB>();

        /// <summary>
        /// For Indexing.
        /// </summary>
        public ConcurrentDictionary<long, EDCommodity_EDDB> commodityIndex = new ConcurrentDictionary<long, EDCommodity_EDDB>();

        
        /// <summary>
        /// For trading.
        /// </summary>
        public ConcurrentDictionary<long, CommodityTrades> commodityTradeIndex = new ConcurrentDictionary<long, CommodityTrades>();

        /// <summary>
        /// Stops all processing due to exception.
        /// </summary>
        public Exception mainException = null;

        /// <summary>
        /// The markets for specific stations. ( not all, only the ones yielding profitable routes. )
        /// Keyed by station_id.
        /// </summary>
        public ConcurrentDictionary<long, List<EDTrade_EDDB>> markets = new ConcurrentDictionary<long, List<EDTrade_EDDB>>();

        /// <summary>
        /// The process time.
        /// </summary>
        public Dictionary<string, string> processTimer = new Dictionary<string, string>();
        public List<string> processTimerKeys = new List<string>();

        /// <summary>
        /// Parse and update data from EDDB to Main DB.
        /// </summary>
        public void ParseAndUpdate(bool trades, bool export)
        {
            string msg = $"{GetType().Name}.ParseAndUpdate";
            Logger.Debug($"{msg}:-- START, MinimumDemand: {MinimumDemand}, MinimumLandingPadSize: {MinimumLandingPadSize}, MaximumTradeDataAgeInDays: {MaximumTradeDataAgeInHours}, " +
                $"MinimumProfit: {MinimumProfit}, MaximumDistanceBetweenTradeSystems: {MaximumDistanceBetweenTradeSystems}, MaximumDistanceFromStar: {MaximumDistanceFromStar}, MaximumCargoSpace: {MaximumCargoSpace}");
            
            string result = "NOT OK";
            DataUtil.Instance.StartTimer(msg);
            
            try
            {
                Systems = new List<EDSystem_EDDB>();
                Stations = new List<EDStation_EDDB>();
                Commodities = new List<EDCommodity_EDDB>();
                Trades = new List<EDTrade_EDDB>();
                systemIndex = new ConcurrentDictionary<long, EDSystem_EDDB>();
                stationIndex = new ConcurrentDictionary<long, EDStation_EDDB>();
                commodityIndex = new ConcurrentDictionary<long, EDCommodity_EDDB>();
                commodityTradeIndex = new ConcurrentDictionary<long, CommodityTrades>();
                markets = new ConcurrentDictionary<long, List<EDTrade_EDDB>>();

                // parse file data into memory from eddb structure
                ParseAll(trades);

                // Debug some
                ShowEDDB();
                
                // Show me the money!
                if (export) ShowMeTheMoney();

                result = "OK";
            } //try
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg}...{result} - {timer}");
            } //finally

            try
            {
                // Write the stats file.
                if (export) WriteStatsFile();
            }
            catch (Exception ex)
            {
                Logger.Warning($"{GetType().Name}.ParseAndUpdate WARNING Could not write Stats File: {EDDB.DataStatsCSV}, {ex}");
                throw;
            }
        }

        /// <summary>
        /// Finds the trade output csv header index by name.
        /// </summary>
        public static int FindTradeOutputCSVHeaderIndex(string name)
        {
            return TRADE_CSV_HEADER.Split(",").ToList().IndexOf(name);
        }

        /// <summary>
        /// Writes the stats file.
        /// </summary>
        private void WriteStatsFile()
        {
            List<string> lines = new List<string>();
            lines.Add(DataStatsCSVHeader);
            lines.Add($"{Systems.Count},{Stations.Count},{Commodities.Count},{Trades.Count},{DateTimeOffset.UtcNow.ToString(DataUtil.DateFormat)}");
            lines.Add("");
            processTimerKeys.ForEach(x =>
            {
                lines.Add($"\"{x}\", {processTimer[x]}");
            });
            
            if (File.Exists(EDDB.SystemsFile))
            {
                FileInfo fi = new FileInfo(EDDB.SystemsFile);
                lines.Add($"{EDDB.SystemsFile},{fi.LastWriteTimeUtc.ToString("yyyy-MM-dd HH-mm-ss")}, {DataUtil.FormatSize(fi.Length)}");
            }
            if (File.Exists(EDDB.StationsFile))
            {
                FileInfo fi = new FileInfo(EDDB.StationsFile);
                lines.Add($"{EDDB.StationsFile},{fi.LastWriteTimeUtc.ToString("yyyy-MM-dd HH-mm-ss")}, {DataUtil.FormatSize(fi.Length)}");
            }
            if (File.Exists(EDDB.CommoditiesFile))
            {
                FileInfo fi = new FileInfo(EDDB.CommoditiesFile);
                lines.Add($"{EDDB.CommoditiesFile},{fi.LastWriteTimeUtc.ToString("yyyy-MM-dd HH-mm-ss")}, {DataUtil.FormatSize(fi.Length)}");
            }
            if (File.Exists(EDDB.TradeFile))
            {
                FileInfo fi = new FileInfo(EDDB.TradeFile);
                lines.Add($"{EDDB.TradeFile},{fi.LastWriteTimeUtc.ToString("yyyy-MM-dd HH-mm-ss")}, {DataUtil.FormatSize(fi.Length)}");
            }
            File.WriteAllLines(EDDB.DataStatsCSV, lines);
        }
        
        
        /// <summary>
        /// Show me the money.
        /// </summary>
        private void ShowMeTheMoney()
        {
            string msg = $"{GetType().Name}.ShowMeTheMoney";
            DataUtil.Instance.StartTimer(msg);
            Logger.Debug($"{msg}:-- START");
            
            try
            {
                List<string> lines = new List<string>();
                // Find the highest sell trades, then look for their matching lowest buy trades
                // and then filter using distance between systems, from star, profit margin, supply and demmand
                Trades.Where(trade =>
                    trade.Sell_price > 0 &&
                    trade.Demand > MinimumDemand &&
                    trade.Station.distance_to_star < MaximumDistanceFromStar &&
                    commodityTradeIndex.ContainsKey(trade.Commodity_id))
                    .OrderByDescending(x => x.Sell_price).ToList().ForEach(sell =>
                    {
                        CommodityTrades buys = commodityTradeIndex[sell.Commodity_id];
                        buys.BuyOrderTrades.Where(buy => (
                            buy.Buy_price > 0 &&
                            buy.Station.distance_to_star < MaximumDistanceFromStar) &&
                            buy.Supply > MinimumDemand &&
                            (sell.Sell_price - buy.Buy_price) > MinimumProfit &&
                            buy.System.DistanceTo(sell.System) <
                            MaximumDistanceBetweenTradeSystems)
                            .OrderByDescending(buy => (sell.Sell_price - buy.Buy_price)).ToList().ForEach(buy =>
                            {
                                float dist = buy.System.DistanceTo(sell.System);
                                float profit = (float)(sell.Sell_price - buy.Buy_price);

                                // find a return trade
                                ReturnTripTrade returntrade = FindReturnTrade(buy, sell);
                                string returnString = "N/A";
                                if (returntrade != null)
                                {
                                    returnString = $"{returntrade.Buy.Commodity.name}: ${returntrade.Profit}";
                                }

                                float cargoprofit = (profit + (returntrade?.Profit ?? 0)) * MaximumCargoSpace;


                                Logger.Info(
                                    $"B: {buy.Commodity.name}({buy.Commodity_id}) Sta: {buy.System.name,-12}:{buy.Station.name,-20}({buy.Station_id})({DataUtil.Number(buy.Station.distance_to_star),-5}ls) ${DataUtil.Number(buy.Buy_price),7}, Sup: {buy.Supply,7}, " +
                                    $"S: {sell.System.name,-12} Sta:{sell.Station.name,-20}({sell.Station_id})({DataUtil.Number(sell.Station.distance_to_star),-5}ls) ${DataUtil.Number(sell.Sell_price),7}, Dmnd: {sell.Demand,7}, " +
                                    $"Dst: {DataUtil.Number(dist),10}, +$: {DataUtil.Number(profit),7}, Ret: {returnString,-30}");

                                lines.Add(
                                    $"{buy.Commodity.name},{buy.System.name},{buy.Station.name},{DataUtil.Number(buy.Station.distance_to_star)},{DataUtil.Number(buy.Buy_price)},{buy.Supply}," +
                                    $"{DataUtil.Number((DateTimeOffset.UtcNow - buy.Collected_at_Date).TotalHours)}," +
                                    $"{sell.System.name},{sell.Station.name},{DataUtil.Number(sell.Station.distance_to_star)},{DataUtil.Number(sell.Sell_price)},{sell.Demand}," +
                                    $"{DataUtil.Number((DateTimeOffset.UtcNow - sell.Collected_at_Date).TotalHours)}," +
                                    $"{DataUtil.Number(dist)},{returnString},{DataUtil.Number(profit)},{DataUtil.Number(profit + (returntrade?.Profit ?? 0))},{DataUtil.Number(cargoprofit / 1000000.0f)}");
                            });
                    });

                // sort by profit for the main output
                lines.Sort((a, b) =>
                {
                    string[] arra = a.Split(",");
                    string[] arrb = b.Split(",");

                    Logger.Trace("======= 1 =========== " + arra[arra.Length - 2]);
                    Logger.Trace("======= 2 =========== " + arrb[arrb.Length - 2]);

                    double num1 = double.Parse(arra[arra.Length - 2]);
                    double num2 = double.Parse(arrb[arrb.Length - 2]);

                    return num2.CompareTo(num1);
                });

                {
                    // write the output trades 
                    using StreamWriter outputFile = new StreamWriter(EDDB.TradeOutputCSV);
                    outputFile.WriteLine(TRADE_CSV_HEADER);
                    lines.ForEach(x => outputFile.WriteLine(x));
                    outputFile.WriteLine($"{DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")}");
                    outputFile.Close();
                }

                {
                    // write the output trades 
                    using StreamWriter outputFile = new StreamWriter(EDDB.ShortJumpTradeOutputCSV);
                    outputFile.WriteLine(TRADE_CSV_HEADER);
                    lines.Where(x =>
                    {
                        string[] arra = x.Split(",");

                        double a_jump = double.Parse(arra[13]);
                        double a_buydist = double.Parse(arra[3]);
                        double a_selldist = double.Parse(arra[9]);

                        return a_jump < 18.6 && a_buydist < 200 && a_selldist < 200;
                    }).ToList().ForEach(x => outputFile.WriteLine(x));
                    outputFile.WriteLine($"{DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")}");
                    outputFile.Close();
                }


            }
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg}...- {timer}");
            }
        }

        /// <summary>
        /// Finds the highest possible commodity to trade on the way back ( if any, that matched criteria )
        /// The way back, means [buy must become sell], and [sell must become buy]
        /// </summary>
        public ReturnTripTrade FindReturnTrade(EDTrade_EDDB buy, EDTrade_EDDB sell)
        {
            return FindMostProfitableTrade(sell.Station, buy.Station, 7000);
        }

        /// <summary>
        /// Find the most profitable trade between 2 stations.
        /// </summary>
        public ReturnTripTrade FindMostProfitableTrade(EDStation_EDDB buyStation, EDStation_EDDB sellStation, int minDemand = 7000)
        {
            Logger.Trace($"{GetType().Name}.FindReturnTrade:-- START, buy: {buyStation.System.name} {buyStation.name}, sell: {sellStation.System.name} {sellStation.name}");
            // get the buy and sell market
            // we want to now BUY from the sell market, and SELL to the buy market [ for the return trip ]
            // find a commodity, with at least 7000 supply/demand ( not configurable ) between the 2, with the highest profit margin.
            
            Logger.Trace($"{GetType().Name}.FindReturnTrade get the buy/sell markets.");
            List<EDTrade_EDDB> buyMarket = FindMarket(buyStation.id);
            List<EDTrade_EDDB> sellMarket = FindMarket(sellStation.id);
            Logger.Trace($"{GetType().Name}.FindReturnTrade get the markets....OK: buyMarket commodities: {buyMarket.Count}, sellMarket commodities: {sellMarket.Count}");
            
            // for each commodity in the buy market, with minimum supply and buy_price > 0
            //  check the sell market if it has minimum demand and get the sell price > 0
            //   order by this (sell - buy) profit desc, and grab the first one, if any
            List<ReturnTripTrade> returnTrips = new List<ReturnTripTrade>();
            buyMarket.OrderBy(x => x.Buy_price).Where(x => x.Supply >= minDemand && x.Buy_price > 0).ToList().ForEach(xbuy =>
            {
                EDTrade_EDDB xsell = sellMarket.OrderByDescending(x => x.Sell_price).Where(sell => sell.Commodity_id == xbuy.Commodity_id && sell.Demand >= minDemand && sell.Sell_price > 0).FirstOrDefault();
                float profit = ((float)((xsell?.Sell_price ?? 0) - xbuy.Buy_price));
                if (xsell != null && profit > 0)
                {
                    returnTrips.Add(new ReturnTripTrade()
                    {
                        Buy = xbuy,
                        Sell = xsell,
                        Profit = profit
                    });
                }
            });
            
            // Order the returntrips by profit desc, and return the first one ( OR NULL ).
            returnTrips.Sort((a, b) => b.Profit.CompareTo(a.Profit));
            return returnTrips.FirstOrDefault();
            
        }

        /// <summary>
        /// Finds a market by station.
        /// </summary>
        public List<EDTrade_EDDB> FindMarket(long stationId)
        {
            // commodity and station is guaranteed to be unique, as our cleanup process ensures this.
            if (markets.ContainsKey(stationId)) return markets[stationId];
            markets[stationId] = Trades.Where(x => x.Station_id == stationId).ToList();
            return markets[stationId];
        }


        /// <summary>
        /// Debug some db stuff.
        /// </summary>
        private void ShowEDDB()
        {
            Logger.Info($"{GetType().Name}.ShowEDDB:-- START, Systems: {Systems.Count}, Stations: {Stations.Count}, Commodities: {Commodities.Count}, Trades: {Trades.Count}");
            Systems.ForEach(x => { Logger.Trace($"{GetType().Name}.ShowEDDB System: {x}"); });
            Stations.ForEach(x => { Logger.Trace($"{GetType().Name}.ShowEDDB Station: {x}"); });
            Commodities.ForEach(x => { Logger.Trace($"{GetType().Name}.ShowEDDB Commodity: {x}"); });

            int cnt = 0;
            Trades.ForEach(x =>
            {
                if (cnt++ > 5000) return;
                Logger.Trace($"{GetType().Name}.ShowEDDB Trade: {x}");
            }); 
        }

        /// <summary>
        /// Parse all data from dist into memory.
        /// </summary>
        private void ParseAll(bool trades)
        {
            string msg = $"{GetType().Name}.ParseAll";
            DataUtil.Instance.StartTimer(msg);
            Logger.Debug($"{msg}:-- START");
            string result = "NOT OK";

            try
            {
                // Parse from disk to EDDB structures.
                List<Task> tasks = new List<Task>();
                tasks.Add(Task.Run(async () => { Systems = await ParseSystems<EDSystem_EDDB>(EDDB.SystemsFile); }));
                tasks.Add(Task.Run(async () => { Stations = await ParseSystems<EDStation_EDDB>(EDDB.StationsFile); }));
                tasks.Add(Task.Run(async () => { Commodities = await ParseSystems<EDCommodity_EDDB>(EDDB.CommoditiesFile); }));
                if (trades) tasks.Add(Task.Run(async () => { await ParseTrades(); }));
                Task.WaitAll(tasks.ToArray());

                if (mainException != null) throw mainException;
                
                // Filter crap.
                FilterSystemsAndStations();
                
                // Sort
                Logger.Debug($"{GetType().Name}.ParseAll...SORT...");
                tasks = new List<Task>();
                tasks.Add(Task.Run(() => { Systems.Sort((a, b) => (a?.name ?? "").CompareTo(b?.name)); }));
                tasks.Add(Task.Run(() => { Stations.Sort((a, b) => (a?.name ?? "").CompareTo(b?.name)); }));
                tasks.Add(Task.Run(() => { Commodities.Sort((a, b) => (a?.name ?? "").CompareTo(b?.name)); }));
                tasks.Add(Task.Run(() => { Trades.Sort((a, b) => b.Collected_at.CompareTo(a.Collected_at)); }));
                Task.WaitAll(tasks.ToArray());
                
                result = "OK";
                
                BuildIndices();
                
                // Only keep the latest System_Station_Commodity, discard the rest, as they are old.
                // Trades are now in date desc order.
                FilterTrades();

                BuildCommodityIndex();

                ShowCommodityIndex();
            } //try
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg}...{result} - {timer}");
            } //finally
        }

        /// <summary>
        /// Remove stuff that wont ever be used.
        /// </summary>
        private void FilterSystemsAndStations()
        {
            Stations.RemoveAll(x => x.type.Trim().ToLower().StartsWith("fleet"));
        }

        /// <summary>
        /// For debugging
        /// </summary>
        private void ShowCommodityIndex()
        {
            commodityTradeIndex.Keys.ToList().ForEach(x =>
            {
                Logger.Trace($"Trade: ================================ Buy/Sell");
                int cnt = 0;
                CommodityTrades trd = commodityTradeIndex[x];

                for (int i = 0; i < Math.Min(trd.BuyOrderTrades.Count, 3); i++)
                {
                    EDTrade_EDDB order = trd.BuyOrderTrades[i];
                    Logger.Trace($"Trade: {trd.Commodity.name}, Buy: {order.Buy_price}, At: {order.System.name} : {order.Station.name}");
                }
                for (int i = 0; i < Math.Min(trd.SellOrderTrades.Count, 3); i++)
                {
                    EDTrade_EDDB order = trd.SellOrderTrades[i];
                    Logger.Trace($"Trade: {trd.Commodity.name}, Sell: {order.Sell_price}, At: {order.System.name} : {order.Station.name}");
                }
            });

            EDSystem_EDDB shin = Systems.First(x => x.name.Trim().ToLower().StartsWith("shinrarta dezhra"));
            // Show the highest selling commodities.
            Logger.Trace($"Trade: ================================ HIGHEST ");
            Trades.Where(x => x.Sell_price > 60000 && commodityTradeIndex.ContainsKey(x.Commodity_id)).OrderByDescending(x => x.Sell_price).ToList().ForEach(x =>
            {
                Logger.Trace($"Trade: {x.Commodity.name}, Sell: {x.Sell_price}, At: {x.System.name} : {x.Station.name}, Dist: {x.System.DistanceTo(shin).ToString("n")}");
            });
        }

        /// <summary>
        /// Builds index.
        /// </summary>
        private void BuildCommodityIndex()
        {
            string msg = $"{GetType().Name}.BuildCommodityIndex";
            DataUtil.Instance.StartTimer(msg);
            Logger.Debug($"{msg}:-- START");
            
            try
            {
                Parallel.ForEach(Commodities, commodity =>
                {
                    CommodityTrades trades = new CommodityTrades();
                    trades.Commodity = commodity;
                    
                    List<Task> tasks = new List<Task>();
                    tasks.Add(Task.Run(() => { trades.BuyOrderTrades = FindBuyOrders(commodity); }));
                    tasks.Add(Task.Run(() => { trades.SellOrderTrades = FindSellOrders(commodity); }));
                    Task.WaitAll(tasks.ToArray());
                    if (mainException != null) throw mainException;

                    if (trades.BuyOrderTrades.Count > 0 && trades.SellOrderTrades.Count > 0) commodityTradeIndex[commodity.id] = trades;
                });
            } //try
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg} - commodityTradeIndex: {commodityTradeIndex.Count} - {timer}");
            } //finally
        }

        /// <summary>
        /// Finds buy orders..
        /// </summary>
        private List<EDTrade_EDDB> FindBuyOrders(EDCommodity_EDDB commodity)
        {
            try
            {
                List<EDTrade_EDDB> trades = Trades.Where(x => x.Commodity_id == commodity.id && x.Buy_price > 0).ToList();
                trades.Sort((a, b) => a.Buy_price.CompareTo(b.Buy_price));
                return trades;
            }
            catch (Exception ex)
            {
                mainException = ex;
                throw;
            }
        }

        /// <summary>
        /// Finds sell orders..
        /// </summary>
        private List<EDTrade_EDDB> FindSellOrders(EDCommodity_EDDB commodity)
        {
            try
            {
                List<EDTrade_EDDB> trades = Trades.Where(x => x.Commodity_id == commodity.id && x.Sell_price > 0).ToList();
                trades.Sort((a, b) => b.Sell_price.CompareTo(a.Sell_price));
                return trades;
            }
            catch (Exception ex)
            {
                mainException = ex;
                throw;
            }

        }
        
        /// <summary>
        /// Indexing.
        /// </summary>
        private void BuildIndices()
        {
            string msg = $"{GetType().Name}.BuildIndices";
            DataUtil.Instance.StartTimer(msg);
            Logger.Debug($"{msg}:-- START");
            
            try
            {
                Parallel.ForEach(Systems, system => { systemIndex[system.id] = system; });
                Parallel.ForEach(Stations, system =>
                {
                    stationIndex[system.id] = system;
                    system.System = systemIndex[system.system_id];
                });
                Parallel.ForEach(Commodities, system => { commodityIndex[system.id] = system; });
                Parallel.ForEach(Trades, system =>
                {
                    if (stationIndex.ContainsKey(system.Station_id))
                    {
                        system.Station = stationIndex[system.Station_id];
                        if (systemIndex.ContainsKey(system.Station.system_id))
                        {
                            system.System = systemIndex[system.Station.system_id];
                        }
                    }
                    system.Commodity = commodityIndex[system.Commodity_id];
                });
            } //try
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg} - {timer}");
            } //finally
            
        }

        /// <summary>
        /// Keep only the latest System_Station_Commodity listing.
        /// </summary>
        private void FilterTrades()
        {
            string msg = $"{GetType().Name}.FilterTrades";
            DataUtil.Instance.StartTimer(msg);
            Logger.Debug($"{msg}:-- START");

            int original = Trades.Count;
            try
            {
                Dictionary<string, bool> keys = new Dictionary<string, bool>(); 
                List<EDTrade_EDDB> trades = new List<EDTrade_EDDB>();
                Trades.ForEach(trade =>
                {
                    bool process = true;
                    string key = $"{trade.Station_id}_{trade.Commodity_id}";

                    process = !keys.ContainsKey(key);
                    if (process) process = trade.System != null && trade.Station != null;
                    if (process) process = (trade.Supply >= MinimumDemand || trade.Demand >= MinimumDemand);
                    if (process)
                    {
                        process = !trade.Station.is_planetary && trade.Station.max_landing_pad_size == MinimumLandingPadSize;
                    }
                    
                    if (process)
                    {
                        keys[key] = true;
                        trades.Add(trade);
                    }
                });
                Trades = trades;
            } //try
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg} - Prev: {original}, Now: {Trades.Count}, Diff: {original - Trades.Count} {timer}");
            } //finally

        }

        /// <summary>
        /// Parses data.
        /// </summary>
        private async Task<List<T>> ParseSystems<T>(string file)
        {
            string msg = $"{GetType().Name}.ParseSystems, Type: {typeof(T).Name}, file: {file}";
            DataUtil.Instance.StartTimer(msg);
            Logger.Trace($"{msg}:-- START");
            int count = 0;

            try
            {
                await using FileStream openStream = File.OpenRead(file);
                IEnumerable<T>? systems = await JsonSerializer.DeserializeAsync<IEnumerable<T>>(openStream);
                List<T> returnResult = systems?.ToList() ?? new List<T>();
                count = returnResult.Count;
                return returnResult;
            } //try
            catch (Exception ex)
            {
                mainException = ex;
                throw ex;
            }
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg} - count: {count}, {timer}");
            } //finally
            
        }

        /// <summary>
        /// Parses trades.
        /// </summary>
        private async Task ParseTrades()
        {
            string msg = $"{GetType().Name}.ParseTrades";
            DataUtil.Instance.StartTimer(msg);
            Logger.Debug($"{msg}:-- START");

            try
            {
                ConcurrentQueue<EDTrade_EDDB> trades = new ConcurrentQueue<EDTrade_EDDB>();
                using (StreamReader reader = File.OpenText(EDDB.TradeFile))
                {
                    while (true)
                    {
                        string? result = await reader.ReadLineAsync();
                        if (result == null) break;
                        if (result.Trim().Length > 0 && !result.Trim().StartsWith("id"))
                        {
                            string[] args = result.Split(",");
                            EDTrade_EDDB trade = new EDTrade_EDDB()
                            {
                                Id = long.Parse(args[TradeMappings.IndexOf("id")], NumberStyles.Number),
                                Commodity_id = long.Parse(args[TradeMappings.IndexOf("commodity_id")],
                                    NumberStyles.Number),
                                Station_id = long.Parse(args[TradeMappings.IndexOf("station_id")], NumberStyles.Number),
                                Supply = long.Parse(args[TradeMappings.IndexOf("supply")], NumberStyles.Number),
                                Demand = long.Parse(args[TradeMappings.IndexOf("demand")], NumberStyles.Number),
                                Sell_price = long.Parse(args[TradeMappings.IndexOf("sell_price")], NumberStyles.Number),
                                Buy_price = long.Parse(args[TradeMappings.IndexOf("buy_price")], NumberStyles.Number),
                                Collected_at = long.Parse(args[TradeMappings.IndexOf("collected_at")], NumberStyles.Number)
                            };
                            // discard old trades
                            if ((DateTimeOffset.UtcNow - trade.Collected_at_Date).TotalHours <= MaximumTradeDataAgeInHours)
                            {
                                trades.Enqueue(trade);
                            }
                        }
                    }
                }

                Trades.AddRange(trades.ToArray());
            } //try
            catch (Exception ex)
            {
                mainException = ex;
                throw ex;
            }
            finally
            {
                string timer = DataUtil.Instance.StopTimer(msg);
                processTimer[msg] = timer;
                processTimerKeys.Add(msg);
                Logger.Debug($"{msg} ED Trades: {Trades.Count} - {timer}");
            } //finally
        }
    }


    /// <summary>
    /// Represents EDDB system/station/market data etc.
    /// </summary>
    [Serializable]
    public class EDSystem_EDDB
    {
        public long id { get; set; } = 0L;
        public string name { get; set; } = null;

        public double x { get; set; } = 0.0d;
        public double y { get; set; } = 0.0d;
        public double z { get; set; } = 0.0d;

        public Vector3 Location => new Vector3((float)x, (float)y, (float)z);

        /// <summary>
        /// Distance to another system.
        /// </summary>
        public float DistanceTo(EDSystem_EDDB edSystem)
        {
            return (Location - edSystem.Location).Length();
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"Id: {id}, Name: {name}, Loc: {x}, {y}, {z}";
        }
    }

    /// <summary>
    /// Represents EDDB system/station/market data etc.
    /// </summary>
    [Serializable]
    public class EDStation_EDDB
    {
        public long id { get; set; } = 0L;
        public long system_id { get; set; } = 0L;
        public EDSystem_EDDB System = null;
        
        public string name { get; set; } = null;
        public string type { get; set; } = null;

        public bool is_planetary { get; set; } = false;
        public string max_landing_pad_size { get; set; } = null;
        public long? distance_to_star { get; set; } = 0L;

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"id: {id}, system_id: {system_id}, name: {name}, is_planetary: {is_planetary}, max_landing_pad_size: {max_landing_pad_size}, distance_to_star: {distance_to_star}";
        }
    }


    /// <summary>
    /// Represents EDDB system/station/market data etc.
    /// </summary>
    [Serializable]
    public class EDCommodity_EDDB
    {
        public long id { get; set; } = 0L;
        public string name { get; set; } = null;
        
        /// <inheritdoc/>
        public override string ToString()
        {
            return $"id: {id}, name: {name}";
        }
    }


    /// <summary>
    /// Represents EDDB system/station/market data etc.
    /// </summary>
    [Serializable]
    public class EDTrade_EDDB
    {
        public EDSystem_EDDB System = null;
        public EDStation_EDDB Station = null;
        public EDCommodity_EDDB Commodity = null;
        
        public long Id;
        public long Commodity_id = 0L;
        public long Station_id = 0L;
        
        public long Supply = 0L;
        public long Demand = 0L;
        public double Sell_price = 0.0d;
        public double Buy_price = 0.0d;

        public long Collected_at = 0L;
        public DateTimeOffset Collected_at_Date => DateTimeOffset.FromUnixTimeSeconds(Collected_at);
        

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"id: {Id}, Station_id: {Station_id}, Commodity_id: {Commodity_id}, Supply: {Supply}, Buy_price: {Buy_price}, Demand: {Demand}, Sell_price: {Sell_price}, Collected_at: {Collected_at}, " +
                $"Collected_at_Date: {Collected_at_Date.ToString(DataUtil.DateFormat)}";
        }
        
    }


    public class CommodityTrades
    {
        /// <summary>
        /// The commodity
        /// </summary>
        public EDCommodity_EDDB Commodity = null;
        
        /// <summary>
        /// The buy order > 0 and sorted asc, lowest first.
        /// </summary>
        public List<EDTrade_EDDB> BuyOrderTrades = new List<EDTrade_EDDB>();
        
        /// <summary>
        /// The sell orders > 0 and sorted desc, highest first.
        /// </summary>
        public List<EDTrade_EDDB> SellOrderTrades = new List<EDTrade_EDDB>();
    }

    public class TradeCleanupRecord
    {
        public long Station_id = 0L;
        public long Commodity_id = 0L;
        public DateTimeOffset Date = DateTimeOffset.MinValue;
        public int LineIndex;
    }

    public class ReturnTripTrade
    {
        public EDTrade_EDDB Buy = null;
        public EDTrade_EDDB Sell = null;
        public float Profit = 0.0f;
    }
    
}
