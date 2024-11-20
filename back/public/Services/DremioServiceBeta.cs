using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DiscoData2API.Misc;
using DiscoData2API_Library.Class;
using Microsoft.Extensions.Options;

/////////////////////////////////////////////////////////////////////////////////////////////////////
//This service is using Dremio API to execute SQL queries and return the results.
//We decided not to use this methodoly and replace it with Arrow Flight API.
/////////////////////////////////////////////////////////////////////////////////////////////////////

namespace DiscoData2API.Services
{
    public class DremioServiceBeta
    {
        private readonly ILogger<DremioServiceBeta> _logger;
        private readonly string? _username;
        private readonly string? _password;
        private readonly string? _dremioServer;
        private static readonly HttpClient Client = new HttpClient();
        private string _authToken;
        private const int pollingIntervalMs = 500; // Check every 500ms
        private const int timeoutMs = 10000; // Timeout after 10 seconds
        private int elapsedTime = 0;

        public DremioServiceBeta(IOptions<ConnectionSettingsDremio> dremioSettings, ILogger<DremioServiceBeta> logger)
        {
            _logger = logger;
            _username = dremioSettings.Value.Username;
            _password = dremioSettings.Value.Password;
            _dremioServer = dremioSettings.Value.DremioServer;
            _authToken = string.Empty;
        }

        public async Task<string> ExecuteQuery(string query, int offset = 0, int limit = 200)
        {
            // Login to Dremio and get the token    
            DremioLogin login = await ApiLogin();
            if (string.IsNullOrEmpty(login.Token))
            {
                _logger.LogError("Dremio token is null for login user");
                return "";
            }

            _authToken = login.Token;

            var jobId = await ApiPost<DremioJob>("sql", new { sql = query });

            if (jobId == null)
            {
                _logger.LogError("Dremio job ID is null");
                return "";
            }

            // Poll the job status until it's completed
            while (elapsedTime < timeoutMs)
            {
                var jobStatus = await ApiGet<DremioJobStatus>($"job/{jobId.Id}");

                if (jobStatus.JobState == MyEnum.JobStatus.COMPLETED.ToString())
                {
                    // Job is completed, fetch the results in chunks
                    StringBuilder allResults = new StringBuilder();
                    bool hasMoreResults = true;

                    while (hasMoreResults)
                    {
                        var jobResults = await ApiGet<string>($"job/{jobId.Id}/results?offset={offset}&limit={limit}");

                        if (!string.IsNullOrEmpty(jobResults))
                        {
                            // Append results to the complete results
                            allResults.Append(jobResults);
                            offset += limit;
                            if(offset >= 10000)
                            {
                                hasMoreResults = false;
                            }
                        }
                        else
                        {
                            hasMoreResults = false;
                        }
                    }

                    return allResults.ToString();
                }
                else if (jobStatus.JobState == MyEnum.JobStatus.FAILED.ToString() || jobStatus.JobState == MyEnum.JobStatus.CANCELED.ToString())
                {
                    _logger.LogError($"Dremio Job {jobId.Id} ended with status: {jobStatus.JobState}");
                    return "";
                }

                await Task.Delay(pollingIntervalMs);
                elapsedTime += pollingIntervalMs;
            }

            _logger.LogError($"Job {jobId.Id} timed out.");
            return "";
        }


        public async Task<DremioLogin> ApiLogin()
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

        public async Task<string> ApiGet(string endpoint)
        {
            return await ApiGet<string>(endpoint);
        }

        public async Task<T> ApiGet<T>(string endpoint)
        {
            if (string.IsNullOrEmpty(_authToken))
            {
                throw new InvalidOperationException("You must login first to obtain the authorization token.");
            }

            try
            {
                var url = $"{_dremioServer}/api/v3/{endpoint}";
                Client.DefaultRequestHeaders.Clear();
                Client.DefaultRequestHeaders.Add("Authorization", $"Bearer {_authToken}");

                var response = await Client.GetAsync(url);
                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception($"Request failed with status code: {response.StatusCode}");
                }

                var jsonResponse = await response.Content.ReadAsStringAsync();
                if (typeof(T) == typeof(string))
                {
                    return (T)Convert.ChangeType(jsonResponse, typeof(T));
                }
                else
                {
                    return JsonSerializer.Deserialize<T>(jsonResponse, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    })!;
                }

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
    }
}