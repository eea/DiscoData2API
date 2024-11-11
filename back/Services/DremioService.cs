using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DiscoData2API.Misc;
using Microsoft.Extensions.Options;

namespace DiscoData2API.Services
{
    public class DremioService
    {
        private readonly ILogger<DremioService> _logger;
        private readonly string _username;
        private readonly string _password;
        private readonly string _dremioServer;
        private static readonly HttpClient Client = new HttpClient();

        public DremioService(IOptions<DremioSettings> dremioSettings, ILogger<DremioService> logger)
        {
            _logger = logger;
            _username = dremioSettings.Value.Username;
            _password = dremioSettings.Value.Password;
            _dremioServer = dremioSettings.Value.DremioServer;
        }

        // Example method that might use these settings
        public void ConnectToDremio()
        {
            // Use _username, _password, _dremioServer to make API requests
        }

        public async Task<string?> GetToken()
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

                // Return headers with authorization token
                return responseJson?.Token ?? string.Empty;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting token");
                return string.Empty;
            }
        }

        public class LoginResponse
        {
            [JsonPropertyName("token")]
            public string Token { get; set; }
        }
    }



}