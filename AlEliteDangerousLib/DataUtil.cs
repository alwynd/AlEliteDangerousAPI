using System.Collections.Concurrent;
using AlEliteDangerousLib.Log;

namespace AlEliteDangerousLib.Data;

/// <summary>
/// Does data stuff.
/// </summary>
public class DataUtil
{
    /// <summary>
    /// The data folder.
    /// </summary>
    public static string DATA_FOLDER = "Data";

    /// <summary>
    /// Date format.
    /// </summary>
    public static string DateFormat = "yyyy-MM-dd HH:mm:ss";
    
    /// <summary>
    /// Keeps track of process time.
    /// </summary>
    private ConcurrentDictionary<string, long> processTimerMap = new ConcurrentDictionary<string, long>();
    
    /// <summary>
    /// The singleton instance.
    /// </summary>
    public static DataUtil Instance = new DataUtil();

    /// <summary>
    /// Constructor.
    /// </summary>
    private DataUtil()
    {
        Initialize();
    }
    
    /// <summary>
    /// Initialize this.
    /// </summary>
    private void Initialize()
    {
        Logger.Debug($"{GetType().Name}.Initialize:-- START, DATA_FOLDER: {DATA_FOLDER}");
        EnsureDirectory(DATA_FOLDER);
    }

    /// <summary>
    /// Starts tracking a process time.
    /// </summary>
    public void StartTimer(string processName)
    {
        processTimerMap[processName] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
    
    /// <summary>
    /// Stops and logs a process timer.
    /// </summary>
    public string StopTimer(string processName)
    {
        if (!processTimerMap.ContainsKey(processName)) return "N/A";
        if (processTimerMap.Remove(processName, out var time)) return ($"took: {TimeSpan.FromMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - time)}");
        return "N/A";
    }
    
    /// <summary>
    /// Number format.
    /// </summary>
    public static String Number(long? number)
    {
        return number?.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture) ?? "0.0";
    }
        
    /// <summary>
    /// Number format.
    /// </summary>
    public static String Number(double number)
    {
        return number.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Ensures it exists.
    /// </summary>
    public void EnsureDirectory(string folder)
    {
        Logger.Debug($"{GetType().Name}.EnsureDirectory:-- START, folder: {folder}");
        if (!Directory.Exists(folder)) Directory.CreateDirectory(folder);
    }

    /// <summary>
    /// Is the file older than x hours? OR it does not exist
    /// </summary>
    public bool IsOlderThanHours(string file, int hours = 12)
    {
        bool exists = File.Exists(file);
        FileInfo? info = null;
        if (exists)
        {
            info = new FileInfo(file);
        }
        Logger.Debug($"{GetType().Name}.IsOlderThanHours:-- START, file: {file}, exists: {exists}, LastWriteTimeUtc: {info?.LastWriteTimeUtc}, hours: {hours}");
        
        if (!exists) return true;
        double diff = (DateTime.UtcNow - info.LastWriteTimeUtc).TotalHours;
        Logger.Debug($"{GetType().Name}.IsOlderThanHours file: {file}, exists: {exists}, LastWriteTimeUtc: {info?.LastWriteTimeUtc}, hours: {hours}, diff: {diff}");
        
        return (diff > hours);
    }

    /// <summary>
    /// Esnures the file's folder exists.
    /// </summary>
    public void EnsureFileFolder(string file)
    {
        Logger.Debug($"{GetType().Name}.EnsureDirectory:-- START, folder: {file}");
        EnsureDirectory(Path.GetDirectoryName(file));
    }

    /// <summary>
    /// Format size.
    /// </summary>
    public static string FormatSize(double size)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        int order = 0;
        while (size >= 1024 && order < sizes.Length - 1) 
        {
            order++;
            size /= 1024;
        }
        return String.Format("{0:0.##} {1}", size, sizes[order]);        
    }
}