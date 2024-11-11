using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DiscoData2API.Misc;
using Microsoft.Extensions.Options;

// https://www.dremio.com/resources/tutorials/using-the-rest-api/

namespace DiscoData2API.Services
{
    public class DremioService
    {
        private readonly ILogger<DremioService> _logger;
        private readonly string _username;
        private readonly string _password;
        private readonly string _dremioServer;
        private static readonly HttpClient Client = new HttpClient();
        private string _authToken;

        public DremioService(IOptions<DremioSettings> dremioSettings, ILogger<DremioService> logger)
        {
            _logger = logger;
            _username = dremioSettings.Value.Username;
            _password = dremioSettings.Value.Password;
            _dremioServer = dremioSettings.Value.DremioServer;
        }

        public async Task<bool> ApiLogin()
        {
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
                var responseJson = JsonSerializer.Deserialize<LoginResponse>(responseData);

                if (responseJson == null || string.IsNullOrEmpty(responseJson.Token))
                {
                    _logger.LogWarning("Dremio Token not found in response");
                    return false;
                }

                _authToken = responseJson.Token;
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting token from Dremio.");
                return false;
            }
        }

        public async Task<T> ApiGet<T>(string endpoint)
        {
            if (string.IsNullOrEmpty(_authToken))
            {
                throw new InvalidOperationException("You must login first to obtain the authorization token.");
            }

            try
            {
                Client.DefaultRequestHeaders.Clear();
                Client.DefaultRequestHeaders.Add("Authorization", $"Bearer {_authToken}");
                Client.DefaultRequestHeaders.Add("Content-Type", "application/json");

                var url = $"{_dremioServer}/api/v3/{endpoint}";
                var response = await Client.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception($"Request failed with status code: {response.StatusCode}");
                }

                var responseString = await response.Content.ReadAsStringAsync();
                return JsonSerializer.Deserialize<T>(responseString);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while making GET request to endpoint: {endpoint}");
                throw;
            }
        }

        public async Task<T?> ApiPost<T>(string endpoint, object? body = null)
        {
            try
            {
                // Set up the request content with the JSON body, if provided
                var content = body != null
                    ? new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json")
                    : null;

                // Set up the request URL and headers
                var url = $"{_dremioServer}/api/v3/{endpoint}";
                Client.DefaultRequestHeaders.Clear();
                Client.DefaultRequestHeaders.Add("Authorization", $"Bearer {_authToken}");
                Client.DefaultRequestHeaders.Add("Content-Type", "application/json");

                // Make the POST request
                var response = await Client.PostAsync(url, content);
                response.EnsureSuccessStatusCode(); // Throws if not successful

                // Read and process the response content
                var responseText = await response.Content.ReadAsStringAsync();
                return string.IsNullOrWhiteSpace(responseText) ? default : JsonSerializer.Deserialize<T>(responseText);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while making POST request to {endpoint}: {ex.Message}");
                throw;
            }
        }

        public class LoginResponse
        {
            [JsonPropertyName("token")]
            public string Token { get; set; }
        }
    }
}