namespace DiscoData2API.Class
{
    public class ConnectionSettingsDremio
    {
        public string? Username
        {
            get
            {
                return System.Environment.GetEnvironmentVariable("DREMIO_USER") ?? string.Empty;
            }
        }

        public string? Password {
            get
            {
                return System.Environment.GetEnvironmentVariable("DREMIO_PWD") ?? string.Empty;
            }
        }
        public string? DremioServer {
            get
            {
                return System.Environment.GetEnvironmentVariable("DREMIO_SERVER") ?? string.Empty;
            }
        }
        public string? DremioServerAuth {
            get
            {
                return System.Environment.GetEnvironmentVariable("DREMIO_SERVER_AUTH") ?? string.Empty;
            }

        }
        public int Limit { get; set; }
        public int Timeout { get; set; }

        // Heavy usage configuration
        public int MaxResultSetSize { get; set; } = 1000000; // Maximum rows per query
        public int DefaultPageSize { get; set; } = 10000; // Default pagination size
        public int MaxPageSize { get; set; } = 100000; // Maximum allowed page size
        public int LargeQueryTimeout { get; set; } = 300000; // 5 minutes for large queries
        public int MaxConcurrentQueries { get; set; } = 10; // Concurrent query limit
        public bool EnableQueryThrottling { get; set; } = true; // Enable request throttling
        public int ThrottleDelayMs { get; set; } = 100; // Delay between throttled requests
    }

}
