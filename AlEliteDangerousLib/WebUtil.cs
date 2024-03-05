using AlEliteDangerousLib.Data;
using AlEliteDangerousLib.Log;

namespace AlEliteDangerousLib.Web;

/// <summary>
/// Does web stuff.
/// </summary>
public static class WebUtil
{
    /// <summary>
    /// Performs http head.
    /// </summary>
    public static async Task<Dictionary<string, List<string>>> HttpHead(string url)
    {
        Logger.Debug($"{typeof(WebUtil).Name}.HttpHead:-- START, url: {url}");

        Dictionary<string, List<string>> headers = new Dictionary<string, List<string>>();
        Exception? exception = null;
        
        int retry = 5;
        while (retry-- > 0)
        {
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    HttpResponseMessage response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, url));
                    Logger.Info($"{typeof(WebUtil).Name}.HttpHead url: {url}... response: {response.StatusCode}");

                    if (!response.IsSuccessStatusCode) throw new Exception($"The url did not respond with success: url: {url}... response: {response.StatusCode}"); response.Headers.ToList().ForEach(x =>
                    {
                        Logger.Trace($"{typeof(WebUtil).Name}.HttpHead url: {url}, headers: '{x.Key}' '{string.Join(",", x.Value)}'");
                        headers[x.Key] = x.Value.ToList();
                    });
                } //using

                return headers;
                
            } //try
            catch (Exception ex)
            {
                Logger.Warning($"{typeof(WebUtil).Name}.HttpHead url: {url} failed: {ex}, retry: {retry}");
                exception = ex;
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        }
        
        if (exception != null) throw exception;
        throw new Exception($"{typeof(WebUtil).Name}.HttpHead No return result.");
    }
    
    
    /// <summary>
    /// Downloads a file.
    /// </summary>
    public static async Task HttpDownload(string url, string file)
    {
        Logger.Debug($"{typeof(WebUtil).Name}.HttpDownload:-- START, url: {url}, file: {file}");

        Exception? exception = null;
        int retry = 5;
        while (retry-- > 0)
        {
            try
            {
                DataUtil.Instance.EnsureFileFolder(file);
                using HttpClient client = new HttpClient();
                {
                    using (Stream stream = await client.GetStreamAsync(url))
                    {
                        using (FileStream fileStream = new FileStream(file, FileMode.Create, FileAccess.Write, FileShare.Read, 1024 * 1024))
                        {
                            await stream.CopyToAsync(fileStream);                            
                        } //using
                    } //using
                } //using

                FileInfo info = new FileInfo(file);
                Logger.Debug($"{typeof(WebUtil).Name}.HttpDownload url: {url}, file: {file}...OK, info: {DataUtil.FormatSize(info.Length)}");
                return;

            } //try
            catch (Exception ex)
            {
                Logger.Warning($"{typeof(WebUtil).Name}.HttpDownload url: {url}, file: {file} failed: {ex}, retry: {retry}");
                exception = ex;
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        }
        
        if (exception != null) throw exception;
        throw new Exception($"{typeof(WebUtil).Name}.HttpDownload No return result.");
    }
    
}