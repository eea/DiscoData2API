using Apache.Arrow.Flight;
using System.Text.Json;
using Apache.Arrow;
using Grpc.Net.Client;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using System.Text;
using Microsoft.Extensions.Options;


namespace DiscoData2API.Class
{
    public class DremioService
    {
        private readonly ILogger<DremioService> _logger;
        private readonly string? _username;
        private readonly string? _password;
        private readonly string? _dremioServer;
        private readonly string? _dremioServerAuth;
        private FlightClient _flightClient;
        public readonly int _limit;
        public readonly int _timeout;


        public DremioService(IOptions<ConnectionSettingsDremio> dremioSettings, ILogger<DremioService> logger)
        {
            _logger = logger;
            _username = dremioSettings.Value.Username;
            _password = dremioSettings.Value.Password;
            _dremioServer = dremioSettings.Value.DremioServer;
            _dremioServerAuth = dremioSettings.Value.DremioServerAuth;
            _limit = dremioSettings.Value.Limit;
            _timeout = dremioSettings.Value.Timeout;
            _flightClient = InitializeFlightClient();
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
            var flightInfo = await ConnectArrowFlight(query, cts);
            _logger.LogInformation($"Afer execute ConnectArrowFlight");

            var allResults = new StringBuilder("[");
            await foreach (var batch in StreamRecordBatches(flightInfo.Item1, flightInfo.Item2))
            {
                //Console.WriteLine($"Read batch from flight server: \n {batch}");
                allResults.Append(ConvertRecordBatchToJson(batch));
                await Task.Delay(TimeSpan.FromMilliseconds(5), cts);
            }
            _logger.LogInformation($"After query result");
            allResults.Append(']');

            return allResults.ToString();
        }


        /// <summary>
        /// Connect to Arrow Flight and get FlightInfo for the query
        /// </summary>
        /// <param name="query">The query to run</param>
        /// <param name="cts">The cancellation token</param>
        /// <returns></returns>
        private async Task<(FlightInfo?, Metadata)> ConnectArrowFlight(string query, CancellationToken cts)
        {

            string token = string.Empty;
            Metadata? headers = null;
            FlightDescriptor? descriptor = null;
            FlightInfo? flightInfo = null;
            //checked first if we can connect to dremio
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


            //check if _flightClient is still working
            try
            {
                descriptor = FlightDescriptor.CreateCommandDescriptor("select 1");
                // Fetch FlightInfo for the query
                flightInfo = await _flightClient.GetInfo(descriptor, headers).ResponseAsync.WaitAsync(cts);
            }
            catch
            {
                //otherwise create a new client connection
                _flightClient = InitializeFlightClient();
            }

            // Prepare the FlightDescriptor for the query
            descriptor = FlightDescriptor.CreateCommandDescriptor(query);

            // Fetch FlightInfo for the query
            flightInfo = await _flightClient.GetInfo(descriptor, headers).ResponseAsync.WaitAsync(cts);
            _logger.LogInformation($"ConnectArrowFlight 4");
            return (flightInfo, headers);
        }

        #region Helpers

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

                // Pass the handler to httpclient(from you are calling api)
                // Make the POST request for authentication
                using var client = new HttpClient(clientHandler);
                // client.BaseAddress = new Uri($"{_dremioServer}/apiv2/login");
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

        private FlightClient InitializeFlightClient()
        {
            //var channel = GrpcChannel.ForAddress($"{_dremioServer}");

            var httpHandler = new HttpClientHandler();
            httpHandler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
            var channel = GrpcChannel.ForAddress($"{_dremioServer}", new GrpcChannelOptions { HttpHandler = httpHandler });

            return new FlightClient(channel);
        }

        private static string ConvertRecordBatchToJson(RecordBatch recordBatch)
        {
            var data = new List<Dictionary<string, object>>();

            // Iterate over rows first
            for (int i = 0; i < recordBatch.Length; i++)
            {
                var rowData = new Dictionary<string, object>();
                try
                {
                    // For each row, iterate over columns
                    foreach (var column in recordBatch.Schema.FieldsList.Zip(recordBatch.Arrays, (field, array) => new { field, array }))
                    {
                        string columnName = column.field.Name;

                        switch (column.array)
                        {
                            case Int32Array int32Array:
                                rowData[columnName] = int32Array.Values[i];
                                break;
                            case Int64Array int64Array:
                                rowData[columnName] = int64Array.Values[i];
                                break;
                            case DoubleArray doubleArray:
                                rowData[columnName] = doubleArray.Values[i];
                                break;
                            case Decimal128Array decimal128Array:
                                rowData[columnName] = decimal128Array.GetValue(i) ?? 0;
                                break;
                            case StringArray stringArray:
                                rowData[columnName] = stringArray.GetString(i);
                                break;
                            case Date64Array date64Array:
                                rowData[columnName] = date64Array.Values[i];
                                break;
                            case Date32Array date32Array:
                                rowData[columnName] = date32Array.Values[i];
                                break;
                            case FloatArray floatArray:
                                rowData[columnName] = floatArray.Values[i];
                                break;
                            // Add cases for other array types as needed
                            default:
                                rowData[columnName] = "Unsupported array type";
                                break;
                        }
                    }
                }
                catch
                {
                }

                data.Add(rowData);
            }


            return JsonSerializer.Serialize(data).Replace("[", "").Replace("]", "");
        }

        public async IAsyncEnumerable<RecordBatch> StreamRecordBatches(FlightInfo info, Metadata headers)
        {
            // There might be multiple endpoints hosting part of the data. In simple services,
            // the only endpoint might be the same server we initially queried.
            foreach (var endpoint in info.Endpoints)
            {
                // We may have multiple locations to choose from. Here we choose the first.
                //var download_channel = GrpcChannel.ForAddress(endpoint.Locations.First().Uri);
                //var download_client = new FlightClient(download_channel);

                var stream = _flightClient.GetStream(endpoint.Ticket, headers);

                while (await stream.ResponseStream.MoveNext())
                {
                    yield return stream.ResponseStream.Current;
                }
            }
        }

        #endregion
    }
}