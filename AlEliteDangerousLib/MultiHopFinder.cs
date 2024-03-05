using System.Collections.Concurrent;
using AlEliteDangerousLib.Data;
using AlEliteDangerousLib.Data.EDDB;
using AlEliteDangerousLib.Log;

namespace AlEliteDangerousLib;


/// <summary>
/// Attempts to find multi hops.
/// </summary>
public class MultiHopFinder
{

    /// <summary>
    /// The Maximum Number Of Multi Hops.
    /// </summary>
    public static int MaximumNumberOfMultiHops = 5;
    
    /// <summary>
    /// For lookups.
    /// </summary>
    private EDDBDataTransformer eddb = new EDDBDataTransformer();
    
    /// <summary>
    /// Stations already processed. ( we are not looking for loops )
    /// </summary>
    private ConcurrentDictionary<long, EDStation_EDDB> InEligableStations = new ConcurrentDictionary<long, EDStation_EDDB>();

    /// <summary>
    /// The trade route.
    /// </summary>
    private List<ReturnTripTrade> trips = new List<ReturnTripTrade>();
    
    /// <summary>
    /// Parse the EDDB Lookups.
    /// </summary>
    private void ParseEDDB()
    {
        // Build the lookups
        eddb.ParseAndUpdate(true, false);
    }
    
    /// <summary>
    /// Initiates the process.
    /// </summary>
    public void FindMultiHops()
    {
        string msg = $"{GetType().Name}.FindMultiHops, MaximumNumberOfMultiHops: {MaximumNumberOfMultiHops}";
        DataUtil.Instance.StartTimer(msg);
        Logger.Debug($"{msg}:-- START");
        
        ParseEDDB();
        
        // find the last most profitable trade [ assumes current data is accurate ]
        // remove this station from eligible list ( we not looking for loops ), this becomes "A"
        
        // find all stations within 'MaximumDistanceBetweenTradeSystems' from "A" ( which has market data, and only commodities with supply|demand and price>0 )
        // find the most profitable trade ( buy at "A", sell at "B" )
        // remove "A" from list, "B" becomes "A" and repeat the process for a few jumps.
        ReturnTripTrade trade = FindCurrentMostProfitableTrade();
        
        // These stations are no no longer eligable.
        InEligableStations[trade.Buy.Station.id] = trade.Buy.Station;
        InEligableStations[trade.Sell.Station.id] = trade.Sell.Station;
        trips.Add(trade);

        FindClosestProfitableTradesRecursive(trade.Sell.Station);
        
        // Find a trade back to the starting market. [ if any ]
        if (trips.Count > 1)
        {
            ReturnTripTrade returntrip = eddb.FindMostProfitableTrade(trips[trips.Count-1].Sell.Station, trips[0].Buy.Station);
            if (returntrip != null) trips.Add(returntrip);
        }

        double profit = 0.0d;
        double runningprofit = 0.0d;
        
        trips.ForEach(trip =>
        {
            profit = trip.Sell.Sell_price - trip.Buy.Buy_price;
            runningprofit += profit; 
            
            Logger.Debug($"{GetType().Name}.FindMultiHops Buy {trip.Buy.Commodity.name} from {trip.Buy.System.name}:{trip.Buy.Station.name} at {trip.Buy.Buy_price}, Sell at {trip.Sell.System.name}:{trip.Sell.Station.name} at: {trip.Sell.Sell_price} " +
                         $"for: {profit} ({DataUtil.Number((runningprofit*720.0d)/1000000)}mil)");
        });

        // Show distance from original location.
        if (trips.Count > 0) Logger.Debug($"{GetType().Name}.FindMultiHops Distance back to original port: {DataUtil.Number(trade.Buy.System.DistanceTo(trips[trips.Count-1].Sell.System))}");
        
        List<string> lines = new List<string>();
        float totalprofit = 0.0f;
        
        trips.ForEach(x =>
        {
            float dist = x.Buy.System.DistanceTo(x.Sell.System);
            float profit = (float)(x.Sell.Sell_price - x.Buy.Buy_price);
            totalprofit += profit; 
            
            lines.Add(
                $"{x.Buy.Commodity.name},{x.Buy.System.name},{x.Buy.Station.name},{DataUtil.Number(x.Buy.Station.distance_to_star)},{DataUtil.Number(x.Buy.Buy_price)},{x.Buy.Supply}," +
                $"{DataUtil.Number((DateTimeOffset.UtcNow - x.Buy.Collected_at_Date).TotalHours)}," +
                $"{x.Sell.System.name},{x.Sell.Station.name},{DataUtil.Number(x.Sell.Station.distance_to_star)},{DataUtil.Number(x.Sell.Sell_price)},{x.Sell.Demand}," +
                $"{DataUtil.Number((DateTimeOffset.UtcNow - x.Sell.Collected_at_Date).TotalHours)}," +
                $"{DataUtil.Number(dist)},,{DataUtil.Number(profit)},{DataUtil.Number(totalprofit)},{DataUtil.Number((totalprofit*720) / 1000000.0f)}");
        });
        {
            // write the output trades 
            using StreamWriter outputFile = new StreamWriter(EDDB.TradeMultihopOutputCSV);
            outputFile.WriteLine(EDDBDataTransformer.TRADE_CSV_HEADER);
            lines.ForEach(x => outputFile.WriteLine(x));
            outputFile.WriteLine($"{DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")}");
            outputFile.Close();
        }

        
        string timer = DataUtil.Instance.StopTimer(msg);
        Logger.Debug($"{msg} - {timer}");
        
    }

    /// <summary>
    /// Finds the most profitable trade, in the current trade output csv data.
    /// </summary>
    private ReturnTripTrade FindCurrentMostProfitableTrade()
    {
        if (!File.Exists(EDDB.TradeOutputCSV)) throw new Exception($"{GetType().Name}.FindMultiHops: ERROR: The TradeOutputCSV: {EDDB.TradeOutputCSV} does not exist.");
        
        // skip header
        string[] firstline = File.ReadAllLines(EDDB.TradeOutputCSV)[1].Split(",");

        // get commodity and station
        ReturnTripTrade trade = new ReturnTripTrade();
        trade.Buy = new EDTrade_EDDB();
        trade.Buy.Commodity = eddb.Commodities.FirstOrDefault(x => x.name == firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Commodity")]);
        trade.Buy.Commodity_id = trade.Buy.Commodity.id;
        
        trade.Buy.System = eddb.Systems.FirstOrDefault(x => x.name == firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Buy System")]);
        trade.Buy.Station = eddb.Stations.FirstOrDefault(x => x.system_id == trade.Buy.System.id && x.name == firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Buy Station")]);
        trade.Buy.Station_id = trade.Buy.Station.id;
        
        trade.Buy.Buy_price = double.Parse(firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Buy Price")]);
        trade.Buy.Supply = int.Parse(firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Supply")]);
        
        
        trade.Sell = new EDTrade_EDDB();
        trade.Sell.Commodity = trade.Buy.Commodity;
        trade.Sell.Commodity_id = trade.Buy.Commodity_id;
        
        trade.Sell.System = eddb.Systems.FirstOrDefault(x => x.name == firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Sell System")]);
        trade.Sell.Station = eddb.Stations.FirstOrDefault(x => x.system_id == trade.Sell.System.id && x.name == firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Sell Station")]);
        trade.Sell.Station_id = trade.Sell.Station.id;

        trade.Sell.Sell_price = double.Parse(firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Sell Price")]);
        trade.Sell.Demand = int.Parse(firstline[EDDBDataTransformer.FindTradeOutputCSVHeaderIndex("Demand")]);
        return trade;
    }

    /// <summary>
    /// Find most profitable trade from here, within jump range, and all other parameters etc.
    /// </summary>
    private void FindClosestProfitableTradesRecursive(EDStation_EDDB buyStation)
    {
        Logger.Debug($"{GetType().Name}.FindClosestProfitableTradesRecursive:-- START, buyStation: {buyStation.System.name}:{buyStation.name}");
        if (trips.Count > MaximumNumberOfMultiHops)
        {
            // we have reached the max hops for this query.
            Logger.Info($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, trips: {trips.Count}, Maximum hops found....");
            return;
        }
        
        // Find the most profitable trade between these.
        List<EDTrade_EDDB> buyMarket = eddb.FindMarket(buyStation.id);
        if (buyMarket.Count < 1)
        {
            // we have reached the max hops for this query.
            Logger.Info($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, trips: {trips.Count}, Buy Market contains no commodities.");
            return;
        }
        Logger.Debug($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, buyMarket: {buyMarket.Count}, looking for eligible stations within {EDDBDataTransformer.MaximumDistanceBetweenTradeSystems} jump distance...");
        
        // Find all closest eligible stations within the jump range.
        // get the markets of both, perform profitable query against the sellStation
        List<EDStation_EDDB> eligibleStations = eddb.Stations.Where(station =>
        {
            // not be ineligible
            // must not be within the same system
            // must not be the same station
            // must be planetary, and have correct landing pads
            // must be within distance from star
            // must be within jump range
            
            bool eligible = !InEligableStations.ContainsKey(station.id); 
            if (!eligible) return false;

            eligible = station.system_id != buyStation.system_id;
            if (!eligible) return false;
            
            eligible = station.id != buyStation.id;
            if (!eligible) return false;

            eligible = !station.is_planetary && station.max_landing_pad_size == EDDBDataTransformer.MinimumLandingPadSize;
            if (!eligible) return false;

            eligible = station.distance_to_star <= EDDBDataTransformer.MaximumDistanceFromStar; 
            if (!eligible) return false;

            double jumpdistance = buyStation.System.DistanceTo(station.System);
            eligible = jumpdistance <= EDDBDataTransformer.MaximumDistanceBetweenTradeSystems;
            if (!eligible) return false;
            
            return eligible;
        }).OrderBy(x => buyStation.System.DistanceTo(x.System)).ToList();

        // this will contain all the eligible markets, in no particular order.
        List<EDTrade_EDDB> sellMarkets = new List<EDTrade_EDDB>();
        Parallel.ForEach(eligibleStations, sellStation =>
        {
            // Log the station
            // find the market for this station, this basically just ensures the market cache
            Logger.Trace($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, buyMarket: {buyMarket.Count}, eligible station: {sellStation.System.name}:{sellStation.name}, Dist: {buyStation.System.DistanceTo(sellStation.System)}");
            eddb.FindMarket(sellStation.id);
        });
        // now synchronous add to list. [ direct concurrent dictionary lookup ]
        eligibleStations.ForEach(x =>
        {
            sellMarkets.AddRange(eddb.FindMarket(x.id));
        });

        Logger.Trace($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, buyMarket: {buyMarket.Count}, comparing against sellMarkets: {sellMarkets.Count}");
        if (sellMarkets.Count < 1)
        {
            Logger.Info($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, buyMarket: {buyMarket.Count}, trips: {trips.Count}, No further eligible markets found, in any of the closest stations.");
            return;
        }

        // now find the most profitable trade against the buy and sell markets.
        List<ReturnTripTrade> sells = new List<ReturnTripTrade>();
        buyMarket.Where(buy => buy.Supply >= EDDBDataTransformer.MinimumDemand && buy.Buy_price > 0)
            .OrderBy(buy => buy.Buy_price).ToList().ForEach(buy =>
            {
                EDTrade_EDDB sell = sellMarkets.OrderByDescending(sell => sell.Sell_price).FirstOrDefault(sell => 
                    sell.Commodity_id == buy.Commodity_id && 
                    sell.Demand >= EDDBDataTransformer.MinimumDemand && 
                    sell.Sell_price > 0);
                if (sell != null)
                {
                    ReturnTripTrade trade = new ReturnTripTrade();
                    trade.Buy = buy;
                    trade.Sell = sell;
                    trade.Profit = (float)(trade.Sell.Sell_price - trade.Buy.Buy_price);
                    sells.Add(trade);
                }
            });

        sells.Sort((a,b) => b.Profit.CompareTo(a.Profit));
        if (sells.Count < 1)
        {
            Logger.Info($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, trips: {trips.Count}, buyMarket: {buyMarket.Count} No eligible sell trades found for any commodity in the buy market.");
            return;
        }
        
        // show the profit...
        // sells.ForEach(x =>
        // {
        //     Logger.Trace($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, buyMarket: {buyMarket.Count}, Commodity: {x.Sell.Commodity.name}, " +
        //         $"Price: {x.Buy.Buy_price} Sell: {x.Sell.System.name}:{x.Sell.Station.name} for {x.Sell.Sell_price}, Profit: {DataUtil.Number(x.Profit)}");
        // });
        Logger.Trace($"{GetType().Name}.FindClosestProfitableTradesRecursive buyStation: {buyStation.System.name}:{buyStation.name}, buyMarket: {buyMarket.Count}, Commodity: {sells[0].Sell.Commodity.name}, " +
            $"Price: {sells[0].Buy.Buy_price} Sell: {sells[0].Sell.System.name}:{sells[0].Sell.Station.name} for {sells[0].Sell.Sell_price}, Profit: {DataUtil.Number(sells[0].Profit)}");

        // These stations are no no longer eligable.
        InEligableStations[sells[0].Buy.Station.id] = sells[0].Buy.Station;
        InEligableStations[sells[0].Sell.Station.id] = sells[0].Sell.Station;
        trips.Add(sells[0]);
        FindClosestProfitableTradesRecursive(sells[0].Sell.Station);
    }

    /// <summary>
    /// Ignores most variables, looks for the highest possible trades.
    /// </summary>
    public void FindHighestPossibleTrade()
    {
        ParseEDDB();
        
        List<ReturnTripTrade> trades = new List<ReturnTripTrade>();
        List<EDTrade_EDDB> markets = new List<EDTrade_EDDB>();
        
        List<EDStation_EDDB> stations = eddb.Stations.Where(station => !station.is_planetary && station.max_landing_pad_size == EDDBDataTransformer.MinimumLandingPadSize && station.distance_to_star <= 10000).ToList();
        Parallel.ForEach(stations, station =>
        {
            eddb.FindMarket(station.id);
        });

        stations.ForEach(station => markets.AddRange(eddb.FindMarket(station.id)));
        markets.OrderBy(x => x.Buy_price).Where(buy => buy.Supply >= 3000 && buy.Buy_price > 0).ToList().ForEach(buy =>
        {
            EDTrade_EDDB sell = markets.Where(sell => sell.Commodity_id == buy.Commodity_id && sell.Demand >= 3000 && sell.Sell_price > 0).OrderByDescending(x => x.Sell_price).FirstOrDefault();
            if (sell != null)
            {
                ReturnTripTrade trade = new ReturnTripTrade()
                {
                    Buy = buy,
                    Sell = sell,
                    Profit = (float)(sell.Sell_price - buy.Buy_price)
                };
                if (trade.Profit >= EDDBDataTransformer.MinimumProfit) trades.Add(trade);
            }
        });

        trades.Sort((a,b) => b.Profit.CompareTo(a.Profit));
        trades.ForEach(x =>
        {
            Logger.Debug($"{GetType().Name}.FindHighestPossibleTrade Commodity: {x.Buy.Commodity.name, -10}, buy: {x.Buy.System.name, -10}:{x.Buy.Station.name, -10}, sell: {x.Sell.System.name, -10}:{x.Sell.Station.name, -10}, " +
                $"Dist: {DataUtil.Number(x.Buy.Station.System.DistanceTo(x.Sell.System)), 5}ly, Profit: {DataUtil.Number(x.Profit), 10}, Cargo: ${DataUtil.Number((x.Profit * 720)/1000000), 10}mil");
        });

        List<string> lines = new List<string>();
        
        trades.ForEach(x =>
        {
            float dist = x.Buy.System.DistanceTo(x.Sell.System);
            float profit = (float)(x.Sell.Sell_price - x.Buy.Buy_price);
            
            lines.Add(
                $"{x.Buy.Commodity.name},{x.Buy.System.name},{x.Buy.Station.name},{DataUtil.Number(x.Buy.Station.distance_to_star)},{DataUtil.Number(x.Buy.Buy_price)},{x.Buy.Supply}," +
                $"{DataUtil.Number((DateTimeOffset.UtcNow - x.Buy.Collected_at_Date).TotalHours)}," +
                $"{x.Sell.System.name},{x.Sell.Station.name},{DataUtil.Number(x.Sell.Station.distance_to_star)},{DataUtil.Number(x.Sell.Sell_price)},{x.Sell.Demand}," +
                $"{DataUtil.Number((DateTimeOffset.UtcNow - x.Sell.Collected_at_Date).TotalHours)}," +
                $"{DataUtil.Number(dist)},,,{DataUtil.Number(profit)},{DataUtil.Number((profit*720) / 1000000.0f)}");
        });
        {
            // write the output trades 
            using StreamWriter outputFile = new StreamWriter(EDDB.TradeHighestOutputCSV);
            outputFile.WriteLine(EDDBDataTransformer.TRADE_CSV_HEADER);
            lines.ForEach(x => outputFile.WriteLine(x));
            outputFile.WriteLine($"{DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")}");
            outputFile.Close();
        }

    }
}