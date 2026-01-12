using Apache.Arrow.Flight;
using System.Text.Json;
using Apache.Arrow;
using Grpc.Net.Client;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using System.Text;
using Microsoft.Extensions.Options;
using DiscoData2API_Priv.Class;
using System.Net.Http;
using System.Collections.Generic;

namespace DiscoData2API_Priv.Services
{
    public class DremioService
    {
        private readonly ILogger<DremioService> _logger;
        private readonly string? _username;
        private readonly string? _password;
        private readonly string? _dremioServer;
        private readonly string? _dremioServerAuth;
        private readonly FlightClientPool _flightClientPool;
        private readonly ConnectionSettingsDremio _settings;
        private readonly CircuitBreaker _circuitBreaker;
        public readonly int _limit;
        public readonly int _timeout;


        public DremioService(IOptions<ConnectionSettingsDremio> dremioSettings, ILogger<DremioService> logger, FlightClientPool flightClientPool, CircuitBreakerService circuitBreakerService)
        {
            _logger = logger;
            _settings = dremioSettings.Value;
            _username = _settings.Username;
            _password = _settings.Password;
            _dremioServer = _settings.DremioServer;
            _dremioServerAuth = _settings.DremioServerAuth;
            _limit = _settings.Limit;
            _timeout = _settings.Timeout;
            _flightClientPool = flightClientPool;
            _circuitBreaker = circuitBreakerService.GetCircuitBreaker("dremio-connection", 3, TimeSpan.FromMinutes(2));
        }



        /// <summary>
        /// Execute a query on Dremio and return the results as a JSON string
        /// </summary>
        /// <param name="query">The query to execute</param>
        /// <param name="cts">The Cancellation token</param>
        /// <returns></returns>
        public async Task<string> ExecuteQuery(string query, CancellationToken cts)
        {
            _logger.LogInformation($"Start execute query");
            var flightConnection = await ConnectArrowFlight(query, cts);
            _logger.LogInformation($"After execute ConnectArrowFlight");

            FlightClient? flightClient = flightConnection.Item3;
            try
            {
                var allResults = new StringBuilder("[");
                await foreach (var batch in StreamRecordBatches(flightConnection.Item1, flightConnection.Item2, flightClient))
                {
                    allResults.Append(ConvertRecordBatchToJson(batch));
                    await Task.Delay(TimeSpan.FromMilliseconds(5), cts);
                }
                _logger.LogInformation($"After query result");
                allResults.Append(']');

                try
                {
                    var action = new FlightAction("Stop Flight Server", new byte[0]);
                    using var call = flightClient.DoAction(action);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error stopping flight server: {ex.Message}");
                }

                return allResults.ToString();
            }
            finally
            {
                // Return client to pool
                _flightClientPool.ReturnClient(flightClient);
            }
        }

        /// <summary>
        /// Execute a query on Dremio and stream results directly to the output stream
        /// </summary>
        /// <param name="query">The query to execute</param>
        /// <param name="outputStream">The stream to write JSON results to</param>
        /// <param name="cts">The Cancellation token</param>
        /// <param name="maxRows">Maximum number of rows to return (0 = unlimited)</param>
        /// <returns>Number of rows processed</returns>
        public async Task<int> ExecuteQueryStream(string query, Stream outputStream, CancellationToken cts, int maxRows = 0)
        {
            // Apply result size limits based on configuration
            var effectiveMaxRows = ApplyResultSizeLimits(maxRows);

            _logger.LogInformation($"Start execute streaming query, maxRows: {effectiveMaxRows}");
            var flightConnection = await ConnectArrowFlight(query, cts);
            _logger.LogInformation($"After execute ConnectArrowFlight");

            FlightClient? flightClient = flightConnection.Item3;
            using var writer = new StreamWriter(outputStream, leaveOpen: true);
            await writer.WriteAsync("[");
            await writer.FlushAsync();

            int totalRows = 0;
            bool first = true;

            try
            {
                await foreach (var batch in StreamRecordBatches(flightConnection.Item1, flightConnection.Item2, flightClient))
                {
                    cts.ThrowIfCancellationRequested();

                    var batchJson = ConvertRecordBatchToJsonArray(batch);

                    foreach (var rowJson in batchJson)
                    {
                        if (effectiveMaxRows > 0 && totalRows >= effectiveMaxRows)
                        {
                            _logger.LogInformation($"Reached max rows limit: {effectiveMaxRows}");
                            goto EndStream;
                        }

                        if (!first)
                        {
                            await writer.WriteAsync(",");
                        }
                        first = false;

                        await writer.WriteAsync(rowJson);
                        totalRows++;

                        // Flush periodically to ensure responsive streaming
                        if (totalRows % 100 == 0)
                        {
                            await writer.FlushAsync();
                        }
                    }

                    // Small delay to prevent overwhelming the client
                    await Task.Delay(TimeSpan.FromMilliseconds(1), cts);
                }

                EndStream:
                await writer.WriteAsync("]");
                await writer.FlushAsync();

                _logger.LogInformation($"Streaming completed, total rows: {totalRows}");
            }
            finally
            {
                // Clean up flight connection
                try
                {
                    var action = new FlightAction("Stop Flight Server", new byte[0]);
                    using var call = flightClient.DoAction(action);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error stopping flight server: {ex.Message}");
                }

                // Return client to pool
                _flightClientPool.ReturnClient(flightClient);
            }

            return totalRows;
        }

        /// <summary>
        /// Execute query for WISE API with Dremio-compatible response format
        /// </summary>
        /// <param name="query">SQL query to execute</param>
        /// <param name="cts">Cancellation token</param>
        /// <returns>Dremio-compatible JSON object with columns and rows</returns>
        public async Task<object> ExecuteWiseQuery(string query, CancellationToken cts)
        {
            _logger.LogInformation($"Start execute WISE query");
            var flightConnection = await ConnectArrowFlight(query, cts);
            _logger.LogInformation($"After execute ConnectArrowFlight");

            FlightClient? flightClient = flightConnection.Item3;
            try
            {
                var columns = new List<object>();
                var rows = new List<object>();
                bool columnsInitialized = false;

                await foreach (var batch in StreamRecordBatches(flightConnection.Item1, flightConnection.Item2, flightClient))
                {
                    // Initialize columns from first batch
                    if (!columnsInitialized)
                    {
                        foreach (var field in batch.Schema.FieldsList)
                        {
                            columns.Add(new { name = field.Name });
                        }
                        columnsInitialized = true;
                    }

                    // Process each row in the batch
                    for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
                    {
                        var rowValues = new List<object>();

                        // For each row, iterate over columns
                        for (int colIndex = 0; colIndex < batch.ColumnCount; colIndex++)
                        {
                            var array = batch.Column(colIndex);
                            object? value;

                            try
                            {
                                value = GetArrayValue(array, rowIndex);
                            }
                            catch (Exception ex)
                            {
                                value = $"Error: {ex.Message}";
                            }

                            rowValues.Add(new { v = value });
                        }

                        rows.Add(new { row = rowValues });
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(5), cts);
                }

                _logger.LogInformation($"After WISE query result");

                try
                {
                    var action = new FlightAction("Stop Flight Server", new byte[0]);
                    using var call = flightClient.DoAction(action);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error stopping flight server: {ex.Message}");
                }

                // Return Dremio-compatible format
                return new
                {
                    columns = columns,
                    rows = rows
                };
            }
            finally
            {
                // Return client to pool
                _flightClientPool.ReturnClient(flightClient);
            }
        }

        /// <summary>
        /// Applies configuration-based result size limits
        /// </summary>
        /// <param name="requestedLimit">The limit requested by the client</param>
        /// <returns>The effective limit to apply</returns>
        private int ApplyResultSizeLimits(int requestedLimit)
        {
            if (requestedLimit <= 0)
            {
                // No limit requested, use default page size
                return _settings.DefaultPageSize;
            }

            // Enforce maximum result set size
            if (requestedLimit > _settings.MaxResultSetSize)
            {
                _logger.LogWarning($"Requested limit {requestedLimit} exceeds max result set size {_settings.MaxResultSetSize}, applying limit");
                return _settings.MaxResultSetSize;
            }

            // Enforce maximum page size for reasonable limits
            if (requestedLimit > _settings.MaxPageSize)
            {
                _logger.LogInformation($"Requested limit {requestedLimit} exceeds max page size {_settings.MaxPageSize}, applying page limit");
                return _settings.MaxPageSize;
            }

            return requestedLimit;
        }

        /// <summary>
        /// Determines the appropriate timeout based on expected result size
        /// </summary>
        /// <param name="expectedRows">Expected number of rows</param>
        /// <returns>Timeout in milliseconds</returns>
        public int GetTimeoutForQuery(int expectedRows)
        {
            // Use longer timeout for large queries
            if (expectedRows > _settings.DefaultPageSize * 5)
            {
                return _settings.LargeQueryTimeout;
            }

            return _timeout;
        }

        /// <summary>
        /// Get the fields returned by a query
        /// </summary>
        /// <param name="query">The query string</param>
        /// <param name="cts">The cancellation tocken</param>
        /// <returns></returns>
        public async Task<List<Model.Field>> GetSchema(string query, CancellationToken cts)
        {
            List<Model.Field> fieldsList = [];
            var flightConnection = await ConnectArrowFlight(query, cts);
            FlightClient? flightClient = flightConnection.Item3;

            try
            {
                if (flightConnection.Item1 != null)
                {
                    foreach (Field f in flightConnection.Item1.Schema.FieldsList)
                    {
                        Model.Field target_field = new()
                        {
                            Name = f.Name,
                            Description = "",
                            Type = f.DataType.TypeId.ToString(),
                            IsNullable = f.IsNullable
                        };

                        if (f.HasMetadata)
                        {
                            if (f.Metadata.TryGetValue("ARROW:FLIGHT:SQL:TYPE_NAME", out string? metatype))
                                target_field.Type = string.Compare(f.Name, "geometry", true) == 0 ||
                                                    string.Compare(f.Name, "geom", true) == 0 ? "geometry" : metatype;

                            if (f.Metadata.TryGetValue("ARROW:FLIGHT:SQL:PRECISION", out string? metavalue))
                                target_field.ColumnSize = metavalue;
                        }

                        fieldsList.Add(target_field);
                    }
                }
            }
            finally
            {
                // Return client to pool
                _flightClientPool.ReturnClient(flightClient);
            }

            return fieldsList;
        }

        /// <summary>
        /// Connect to Arrow Flight and get FlightInfo for the query
        /// </summary>
        /// <param name="query">The query to run</param>
        /// <param name="cts">The cancellation token</param>
        /// <returns></returns>
        private async Task<(FlightInfo?, Metadata, FlightClient)> ConnectArrowFlight(string query, CancellationToken cts)
        {
            return await _circuitBreaker.ExecuteAsync(async () =>
            {
                string token = string.Empty;
                Metadata? headers = null;
                FlightDescriptor? descriptor = null;
                FlightInfo? flightInfo = null;
                FlightClient? flightClient = null;

                try
                {
                   token = await Authenticate();
                   headers = new Metadata { { "authorization", $"Bearer {token}" } };
                }
                catch
                {
                    throw;
                }
                if (string.IsNullOrEmpty(token))
                    throw new Exception("Cannot connect to Dremio server");

                // Get a client from the pool
                flightClient = _flightClientPool.GetClient();

                //check if flightClient is still working
                try
                {
                    descriptor = FlightDescriptor.CreateCommandDescriptor("select 1");
                    // Fetch FlightInfo for the query
                    flightInfo = await flightClient.GetInfo(descriptor, headers).ResponseAsync.WaitAsync(cts);
                }
                catch
                {
                    // Return the faulty client and get a new one
                    _flightClientPool.ReturnClient(flightClient);
                    flightClient = _flightClientPool.GetClient();
                }

                // Prepare the FlightDescriptor for the query
                descriptor = FlightDescriptor.CreateCommandDescriptor(query);

                // Fetch FlightInfo for the query
                flightInfo = await flightClient.GetInfo(descriptor, headers).ResponseAsync.WaitAsync(cts);
                _logger.LogInformation($"ConnectArrowFlight completed");
                return (flightInfo, headers, flightClient);
            });
        }

        #region Helpers

        /// <summary>
        /// Extract a value from an Arrow array at a specific row index
        /// </summary>
        private static object? GetArrayValue(IArrowArray array, int rowIndex)
        {
            return array switch
            {
                Int8Array int8Array => int8Array.Values[rowIndex],
                Int16Array int16Array => int16Array.Values[rowIndex],
                Int32Array int32Array => int32Array.Values[rowIndex],
                Int64Array int64Array => int64Array.Values[rowIndex],
                UInt8Array uint8Array => uint8Array.Values[rowIndex],
                UInt16Array uint16Array => uint16Array.Values[rowIndex],
                UInt32Array uint32Array => uint32Array.Values[rowIndex],
                UInt64Array uint64Array => uint64Array.Values[rowIndex],
                FloatArray floatArray => floatArray.Values[rowIndex],
                DoubleArray doubleArray => doubleArray.Values[rowIndex],
                BooleanArray boolArray => boolArray.GetValue(rowIndex),
                Decimal128Array decimal128Array => decimal128Array.GetValue(rowIndex) ?? 0,
                Decimal256Array decimal256Array => decimal256Array.GetString(rowIndex),
                StringArray stringArray => stringArray.GetString(rowIndex),
                BinaryArray binaryArray => Convert.ToBase64String(binaryArray.GetBytes(rowIndex).ToArray()),
                Date32Array date32Array => date32Array.Values[rowIndex],
                Date64Array date64Array => date64Array.Values[rowIndex],
                Time32Array time32Array => time32Array.Values[rowIndex],
                Time64Array time64Array => time64Array.Values[rowIndex],
                TimestampArray timestampArray => timestampArray.GetValue(rowIndex),
                NullArray => null,
                _ => $"Unsupported array type: {array.GetType().Name}"
            };
        }

         private async Task<string> Authenticate()
        {
            try
            {
                // Prepare login payload
                var loginData = new { userName = _username, password = _password };
                var jsonLoginData = JsonSerializer.Serialize(loginData);
                var content = new StringContent(jsonLoginData, Encoding.UTF8, "application/json");


                HttpClientHandler clientHandler = new HttpClientHandler();
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };

                // Make the POST request for authentication
                using var client = new HttpClient(clientHandler);
                using var response = await client.PostAsync($"{_dremioServerAuth}/apiv2/login", content);
                response.EnsureSuccessStatusCode();

                var responseContent = await response.Content.ReadAsStringAsync();
                var loginResponse = JsonSerializer.Deserialize<DremioLogin>(responseContent);

                if (loginResponse == null || string.IsNullOrEmpty(loginResponse.Token))
                {
                    throw new Exception("Failed to authenticate with Dremio. Token not received.");
                }                
                return loginResponse.Token;

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while authenticating with Dremio.");
                throw;
            }
        }

        private static string ConvertRecordBatchToJson(RecordBatch recordBatch)
        {
            var data = new List<Dictionary<string, object?>>();

            // Iterate over rows first
            for (int i = 0; i < recordBatch.Length; i++)
            {
                var rowData = new Dictionary<string, object?>();
                try
                {
                    // For each row, iterate over columns
                    foreach (var column in recordBatch.Schema.FieldsList.Zip(recordBatch.Arrays, (field, array) => new { field, array }))
                    {
                        string columnName = column.field.Name;
                        rowData[columnName] = GetArrayValue(column.array, i);
                    }
                }
                catch (Exception)
                {
                    // Skip problematic rows silently to maintain data integrity
                }

                data.Add(rowData);
            }


            return JsonSerializer.Serialize(data).Replace("[", "").Replace("]", "");
        }

        private static List<string> ConvertRecordBatchToJsonArray(RecordBatch recordBatch)
        {
            var results = new List<string>();

            // Iterate over rows first
            for (int i = 0; i < recordBatch.Length; i++)
            {
                var rowData = new Dictionary<string, object?>();
                try
                {
                    // For each row, iterate over columns
                    foreach (var column in recordBatch.Schema.FieldsList.Zip(recordBatch.Arrays, (field, array) => new { field, array }))
                    {
                        string columnName = column.field.Name;
                        rowData[columnName] = GetArrayValue(column.array, i);
                    }

                    results.Add(JsonSerializer.Serialize(rowData));
                }
                catch (Exception ex)
                {
                    // Log the error but continue processing
                    results.Add(JsonSerializer.Serialize(new { error = $"Row processing error: {ex.Message}" }));
                }
            }

            return results;
        }

        public async IAsyncEnumerable<RecordBatch> StreamRecordBatches(FlightInfo info, Metadata headers, FlightClient flightClient)
        {
            // There might be multiple endpoints hosting part of the data. In simple services,
            // the only endpoint might be the same server we initially queried.
            foreach (var endpoint in info.Endpoints)
            {
                var stream = flightClient.GetStream(endpoint.Ticket, headers);

                while (await stream.ResponseStream.MoveNext())
                {
                    yield return stream.ResponseStream.Current;
                }
            }
        }
  
        #endregion
    }
}