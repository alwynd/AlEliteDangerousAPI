// See https://aka.ms/new-console-template for more information

using System.Globalization;
using AlEliteDangerousLib;
using AlEliteDangerousLib.Data;
using AlEliteDangerousLib.Data.EDDB;
using AlEliteDangerousLib.Data.EDDN;
using AlEliteDangerousLib.Log;

/**
 * Note this system cleans and maintains the data automatically, based on UTC date, so no need to periodically stop/start/clean the system.
 * Option eddn shoudl be restarted once a week for best results ( new stations,commoditties etc )
 */

Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");


string msg = $"{typeof(Program)}.Main";
try
{
    DataUtil.Instance.StartTimer(msg);
    EDDB eddb = new EDDB();

    Logger.Log($"{msg}:-- START, Data Folder: {DataUtil.DATA_FOLDER}, Usage: -[eddb|eddn|multi|clean]");
    
    Logger.Log($"==================== EDDB ======================\nUsage: -eddb #This will parse and query the collected eddn and eddbb data for profitable trade routes.\nOptions: " +
               $"-demand [MinimumDemand|20000] -padsize [MinimumLandingPadSize|L] -age [MaximumTradeDataAgeInHours|12] -profit [MinimumProfit|19000] -jumprange [MaximumDistanceBetweenTradeSystems|50] " +
               $"-dist [MaximumDistanceFromStar|500] -cargo [MaximumCargoSpace|720]");
    
    Logger.Log($"==================== EDDB ======================\nUsage: -eddn #This will listen to the eddn queue, and collect only trade data, in the eddb csv format.");
    
    Logger.Log($"==================== CLEAN ======================\nUsage: -clean #This will clean all local data.");

    Logger.Log($"==================== MULTI ======================\nUsage: -multi #This uses the last eddb output, and attempt to find profitable multiple hop trades.\nOptions: " +
               $"-maxhops [MaximumNumberOfMultiHops|5]");
    
    
    List<string> theArgs = args.ToList();

    // argument getters.
    string prev = "";
    for (int i = 0; i < theArgs.Count; i++)
    {
        if (prev == "-demand") EDDBDataTransformer.MinimumDemand = int.Parse(theArgs[i]);
        if (prev == "-padsize") EDDBDataTransformer.MinimumLandingPadSize = theArgs[i];
        if (prev == "-age") EDDBDataTransformer.MaximumTradeDataAgeInHours = int.Parse(theArgs[i]);
        if (prev == "-profit") EDDBDataTransformer.MinimumProfit = int.Parse(theArgs[i]);
        if (prev == "-jumprange") EDDBDataTransformer.MaximumDistanceBetweenTradeSystems = int.Parse(theArgs[i]);
        if (prev == "-dist") EDDBDataTransformer.MaximumDistanceFromStar = int.Parse(theArgs[i]);
        if (prev == "-cargo") EDDBDataTransformer.MaximumCargoSpace = int.Parse(theArgs[i]);
        
        if (prev == "-maxhops") MultiHopFinder.MaximumNumberOfMultiHops = int.Parse(theArgs[i]);
        
        if (prev == "-loglevel") Logger.LogLevel = Enum.Parse<LogLevel>(theArgs[i]);
        prev = theArgs[i];
    }
    
    if (theArgs.Contains("-clean"))
    {
        Logger.Log($"{typeof(Program)}.Main cleaning EDDB");
        eddb.Clean();
        Logger.Log($"{typeof(Program)}.Main cleaning EDDB...OK");
    }

    if (theArgs.Contains("-eddb"))
    {
        Logger.Log($"{typeof(Program)}.Main updating EDDB");
        eddb.Update();
        Logger.Log($"{typeof(Program)}.Main updating EDDB...OK");
    }

    if (theArgs.Contains("-eddn"))
    {
        if (theArgs.Contains("-light"))
        {
            new EDDN().StartFeed(true);
        }
        else
        {
            new EDDN().StartFeed();
        }
    }

    if (theArgs.Contains("-multi"))
    {
        new MultiHopFinder().FindMultiHops();
    }

    if (theArgs.Contains("-highest"))
    {
        new MultiHopFinder().FindHighestPossibleTrade();
    }
    

} //try
catch (Exception ex)
{
    Logger.Error($"{typeof(Program)}.Main: Error: {ex}");
}
finally
{
    Logger.Log($"Main: - {DataUtil.Instance.StopTimer(msg)}");
}
