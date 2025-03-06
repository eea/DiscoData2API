using System.Diagnostics;
using System.Text;
using Prometheus;

namespace DiscoData2API_Priv.Misc
{
    public class PrometheusMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly ILogger _logger;

        //--------------------------------------------------------------------------------
        // 🚀SEB : Modify this methods to add/remove what prometheus is login
        //--------------------------------------------------------------------------------
        private static readonly Summary RequestDurationSummary = Metrics.CreateSummary(
            "INTERNAL_DATAHUB_API_http_request_duration_total_seconds",
            "Total duration of HTTP requests.",
            new SummaryConfiguration
            {
                LabelNames = new[] { "path", "method", "status" }
            });

        //--------------------------------------------------------------------------------
        // 🚀SEB : Modify this methods to add/remove what prometheus is login
        //--------------------------------------------------------------------------------
        private static readonly Counter RecordsReturnedCounter = Metrics.CreateCounter(
            "INTERNAL_DATAHUB_API_http_response_records_total",
            "Total number of records returned in HTTP responses.",
            new CounterConfiguration
            {
                LabelNames = new[] { "path", "method", "status" }
            });

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="next"></param>
        /// <param name="loggerFactory"></param>
        public PrometheusMiddleware(RequestDelegate next, ILoggerFactory loggerFactory)
        {
            _next = next;
            _logger = loggerFactory.CreateLogger<PrometheusMiddleware>();
        }

        /// <summary>
        /// Invoke the middleware when a http request is made.
        /// </summary>
        /// <param name="httpContext"></param>
        /// <returns></returns>
        public async Task Invoke(HttpContext httpContext)
        {
            var path = httpContext.Request.Path.Value;
            var method = httpContext.Request.Method;

            if (path == "/metrics")
            {
                await _next.Invoke(httpContext);
                return; // Don't track Prometheus metrics endpoint
            }

            var stopwatch = Stopwatch.StartNew();
            int statusCode = 200;
            int recordsReturned = 0;

            // Capture the response body
            var originalBodyStream = httpContext.Response.Body;
            using (var memoryStream = new MemoryStream())
            {
                httpContext.Response.Body = memoryStream;

                try
                {
                    await _next.Invoke(httpContext);
                    statusCode = httpContext.Response.StatusCode;

                    // Read response body to count records
                    memoryStream.Seek(0, SeekOrigin.Begin);
                    var responseBody = await new StreamReader(memoryStream, Encoding.UTF8, leaveOpen: true).ReadToEndAsync();
                    recordsReturned = CountJsonRecords(responseBody);

                    memoryStream.Seek(0, SeekOrigin.Begin);
                    await memoryStream.CopyToAsync(originalBodyStream);
                }
                catch (Exception)
                {
                    statusCode = 500;
                    throw;
                }
                finally
                {
                    stopwatch.Stop();
                    httpContext.Response.Body = originalBodyStream;
                }
            }

            // ✅ Observe total request duration WITHOUT buckets
            RequestDurationSummary.WithLabels(path, method, statusCode.ToString()).Observe(stopwatch.Elapsed.TotalSeconds);

            // ✅ Record number of records returned
            RecordsReturnedCounter.WithLabels(path, method, statusCode.ToString()).Inc(recordsReturned);
        }

        /// <summary>
        /// Count the number of JSON records in the response body.
        /// </summary>
        /// <param name="responseBody"></param>
        /// <returns></returns>
        private int CountJsonRecords(string responseBody)
        {
            if (string.IsNullOrWhiteSpace(responseBody))
                return 0;

            if (responseBody.Trim().StartsWith("["))
            {
                return responseBody.Count(c => c == '{');
            }

            return 1;
        }
    }

    #region helper classes

    /// <summary>
    /// Extension method to add the middleware to the HTTP request pipeline.
    /// </summary>
    public static class PrometheusMiddlewareExtensions
    {
        public static IApplicationBuilder UsePrometheusMiddleware(this IApplicationBuilder builder)
        {
            return builder.UseMiddleware<PrometheusMiddleware>();
        }
    }

    #endregion
}