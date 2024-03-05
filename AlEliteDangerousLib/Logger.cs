namespace AlEliteDangerousLib.Log;

/// <summary>
/// The log level. 
/// </summary>
public enum LogLevel
{
    Trace=0, Debug=1, Info=2, Warning=3, Error=4
}


/// <summary>
/// Responsible for logging.
/// </summary>
public static class Logger
{
    /// <summary>
    /// The overall log level.
    /// </summary>
    public static LogLevel LogLevel = LogLevel.Debug;
    
    /// <summary>
    /// Logs to console.
    /// </summary>
    public static void Log(string msg, LogLevel logLevel = LogLevel.Info)
    {
        if ((int)logLevel < (int)LogLevel) return;
        Console.WriteLine($"{DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")} [{logLevel}] {msg}");
    }

    public static void Debug(string msg) => Log(msg, LogLevel.Debug);
    public static void Trace(string msg) => Log(msg, LogLevel.Trace);
    public static void Info(string msg) => Log(msg, LogLevel.Info);
    public static void Warning(string msg) => Log(msg, LogLevel.Warning);
    public static void Error(string msg) => Log(msg, LogLevel.Error);


}