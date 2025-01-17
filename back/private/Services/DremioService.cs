using Apache.Arrow.Flight;
using System.Text.Json;
using Apache.Arrow;
using Grpc.Net.Client;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using System.Text;
using Microsoft.Extensions.Options;
using DiscoData2API.Class;
using DiscoData2API_Priv.Class;

namespace DiscoData2API_Priv.Services
{
    public class DremioService
    {
        private readonly ILogger<DremioService> _logger;
        private readonly string? _username;
        private readonly string? _password;
        private readonly string? _dremioServer;
        private readonly string? _dremioServerAuth;
        private readonly FlightClient _flightClient;
        public readonly int _limit;
        public readonly int _timeout;
        private readonly HttpClient _httpClient;


        public DremioService(IOptions<ConnectionSettingsDremio> dremioSettings, ILogger<DremioService> logger, IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _username = dremioSettings.Value.Username;
            _password = dremioSettings.Value.Password;
            _dremioServer = dremioSettings.Value.DremioServer;
            _dremioServerAuth = dremioSettings.Value.DremioServerAuth;
            _limit = dremioSettings.Value.Limit;
            _timeout = dremioSettings.Value.Timeout;
            _httpClient = httpClientFactory.CreateClient();
            _flightClient = InitializeFlightClient();

        }

        private async Task<(FlightInfo?, Metadata)> ConnectArrowFlight(string query, CancellationToken cts)
        {
            // Authenticate and obtain token
            var token = await Authenticate();
            var headers = new Metadata { { "authorization", $"Bearer {token}" } };

            // Prepare the FlightDescriptor for the query
            var descriptor = FlightDescriptor.CreateCommandDescriptor(query);
            // Fetch FlightInfo for the query
            var flightInfo = await _flightClient.GetInfo(descriptor, headers).ResponseAsync.WaitAsync(cts);

            return (flightInfo, headers);
        }


        public async Task<List<DiscoData2API_Priv.Model.Field >>  GetSchema(string query, CancellationToken cts)
        {
            List<DiscoData2API_Priv.Model.Field> fieldsList = [];
            var flightInfo = await ConnectArrowFlight(query, cts);
            if (flightInfo.Item1 != null)
            {
                foreach (Field f in flightInfo.Item1.Schema.FieldsList)
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
                            target_field.Type = string.Compare(f.Name, "geometry", true) ==0 ||
                                                string.Compare(f.Name, "geom", true ) == 0 ? "geometry" : metatype;

                        if (f.Metadata.TryGetValue("ARROW:FLIGHT:SQL:PRECISION", out string? metavalue))
                            target_field.ColumnSize = metavalue;
                    }

                    fieldsList.Add(target_field);
                }
            }

            return fieldsList;
        }


        public async Task<string> ExecuteQuery(string query, CancellationToken cts)
        {

            var flightInfo = await ConnectArrowFlight(query, cts);

            /*
            // Authenticate and obtain token
            var token = await Authenticate();
            var headers = new Metadata { { "authorization", $"Bearer {token}" } };

            // Prepare the FlightDescriptor for the query
            var descriptor = FlightDescriptor.CreateCommandDescriptor(query);
            // Fetch FlightInfo for the query
            var flightInfo = await _flightClient.GetInfo(descriptor, headers).ResponseAsync.WaitAsync(cts);
            */




            var allResults = new StringBuilder("[");
            await foreach (var batch in StreamRecordBatches(flightInfo.Item1, flightInfo.Item2))
            {
                //Console.WriteLine($"Read batch from flight server: \n {batch}");
                allResults.Append(ConvertRecordBatchToJson(batch));
                await Task.Delay(TimeSpan.FromMilliseconds(10),cts);
            }


            allResults.Append(']');
            return allResults.ToString();
        }

        public async Task<DremioLogin?> ApiLogin()
        {
            try
            {
                // Prepare login data as JSON
                var loginData = new { userName = _username, password = _password };
                var jsonLoginData = JsonSerializer.Serialize(loginData);
                var content = new StringContent(jsonLoginData, Encoding.UTF8, "application/json"); // Set content-type here

                // Make the POST request
                var response = await _httpClient.PostAsync(_dremioServer + "/apiv2/login", content);
                response.EnsureSuccessStatusCode(); // Throw if not a success status code

                // Parse the response to retrieve the token
                var responseData = await response.Content.ReadAsStringAsync();
                var responseJson = JsonSerializer.Deserialize<DremioLogin>(responseData);

                if (responseJson == null || string.IsNullOrEmpty(responseJson.Token))
                {
                    _logger.LogWarning("Dremio Token not found in response");
                    return null;
                }
                return responseJson;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting token from Dremio.");
                return null;
            }
        }

        #region Helpers

        private FlightClient InitializeFlightClient()
        {
            var channel = GrpcChannel.ForAddress($"{_dremioServer}");
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
#pragma warning disable CS8601 // Posible asignaci�n de referencia nula
                                rowData[columnName] = decimal128Array.GetValue(i);
#pragma warning restore CS8601 // Posible asignaci�n de referencia nula
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

        private async Task<string> Authenticate()
        {
            try
            {
                // Prepare login payload
                var loginData = new { userName = _username, password = _password };
                var jsonLoginData = JsonSerializer.Serialize(loginData);
                var content = new StringContent(jsonLoginData, Encoding.UTF8, "application/json");

                // Make the POST request for authentication
                using var client = new HttpClient();
                // client.BaseAddress = new Uri($"{_dremioServer}/apiv2/login");
                var response = await client.PostAsync($"{_dremioServerAuth}/apiv2/login", content);
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

        #endregion

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
    }
}