using Apache.Arrow.Flight;
using System.Text.Json;
using Apache.Arrow;
using Grpc.Net.Client;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using System.Text;
using DiscoData2API_Library.Class;
using Microsoft.Extensions.Options;

namespace DiscoData2API_Priv.Services
{
    public class DremioService
    {
        private readonly ILogger<DremioService> _logger;
        private readonly string? _username;
        private readonly string? _password;
        private readonly string? _dremioServer;
        private string? _dremioServerAuth;
        private FlightClient _flightClient;

        public DremioService(IOptions<ConnectionSettingsDremio> dremioSettings, ILogger<DremioService> logger)
        {
            _logger = logger;
            _username = dremioSettings.Value.Username;
            _password = dremioSettings.Value.Password;
            _dremioServer = dremioSettings.Value.DremioServer;
            _dremioServerAuth = dremioSettings.Value.DremioServerAuth;
            _flightClient = InitializeFlightClient();
        }

        public async Task<string> ExecuteQuery(string query)
        {
            string jsonResult = string.Empty;
            try
            {
                // Authenticate and obtain token
                var token = await Authenticate();
                var headers = new Metadata { { "authorization", $"Bearer {token}" } };

                // Prepare the FlightDescriptor for the query
                var descriptor = FlightDescriptor.CreateCommandDescriptor(query);

                // Fetch FlightInfo for the query
                var flightInfo = await _flightClient.GetInfo(descriptor, headers).ResponseAsync;

                // Iterate over the returned tickets from FlightInfo
                foreach (var endpoint in flightInfo.Endpoints)
                {
                    // Each endpoint provides a ticket for data retrieval
                    var ticket = endpoint.Ticket;

                    // Open a stream for the ticket
                    using var stream = _flightClient.GetStream(ticket, headers);

                    // Process stream of Arrow RecordBatches
                    while (await stream.ResponseStream.MoveNext())
                    {
                        var current = stream.ResponseStream.Current;

                        var arrays = stream.ResponseStream.Current.Arrays.ToList();
                        // Convert RecordBatch to a serializable format
                        jsonResult = ConvertRecordBatchToJson(current);

                        // FOR DEBUGGING: Write the JSON result to a file
                        //await File.WriteAllTextAsync("output.json", jsonResult);
                    }
                }

                return jsonResult;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while executing query via Arrow Flight.");
                throw;
            }
        }

        public async Task<DremioLogin> ApiLogin()
        {
            HttpClient Client = new HttpClient();
            try
            {
                // Prepare login data as JSON
                var loginData = new { userName = _username, password = _password };
                var jsonLoginData = JsonSerializer.Serialize(loginData);
                var content = new StringContent(jsonLoginData, Encoding.UTF8, "application/json"); // Set content-type here

                // Make the POST request
                var response = await Client.PostAsync(_dremioServer + "/apiv2/login", content);
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

        private string ConvertRecordBatchToJson(RecordBatch recordBatch)
        {
            var data = new List<Dictionary<string, object>>();

            // Iterate over rows first
            for (int i = 0; i < recordBatch.Length; i++)
            {
                var rowData = new Dictionary<string, object>();

                // For each row, iterate over columns
                foreach (var column in recordBatch.Schema.Fields.Zip(recordBatch.Arrays, (field, array) => new { field, array }))
                {
                    string columnName = column.field.Value.Name;

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
                            rowData[columnName] = decimal128Array.GetValue(i);
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

                data.Add(rowData);
            }

            return JsonSerializer.Serialize(data);
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
    }
}