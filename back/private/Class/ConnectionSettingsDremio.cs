namespace DiscoData2API_Priv.Class
{
    public class ConnectionSettingsDremio
    {
        private string _dremioServer = string.Empty;
        private string _dremioServerAuth = string.Empty;
        private string _dremioUser = string.Empty;
        private string _dremioPwd = string.Empty;
        public string? Username
        {
            get
            {
                // custom logic when reading
                return Environment.GetEnvironmentVariable("DREMIO_USER") ?? _dremioUser;
            }
            set
            {
                // custom logic when setting
                Console.WriteLine($"Setting Name to {value}");
                if (!string.IsNullOrWhiteSpace(value))
                {
                    _dremioUser = value;
                }
                else
                {
                    throw new ArgumentException("DremioUser cannot be empty");
                }
            }

        }

        public string? Password
        {
            get
            {
                // custom logic when reading
                return Environment.GetEnvironmentVariable("DREMIO_PWD") ?? _dremioPwd;
            }
            set
            {
                // custom logic when setting
                Console.WriteLine($"Setting Name to {value}");
                if (!string.IsNullOrWhiteSpace(value))
                {
                    _dremioPwd = value;
                }
                else
                {
                    throw new ArgumentException("DremioPwd cannot be empty");
                }
            }


        }
        public string? DremioServer
        {
            get
            {
                // custom logic when reading
                return Environment.GetEnvironmentVariable("DREMIO_SERVER") ?? _dremioServer;
            }
            set
            {
                // custom logic when setting
                Console.WriteLine($"Setting Name to {value}");
                if (!string.IsNullOrWhiteSpace(value))
                {
                    _dremioServer = value;
                }
                else
                {
                    throw new ArgumentException("DremioServer cannot be empty");
                }
            }
        }
        public string? DremioServerAuth
        {
            get
            {
                // custom logic when reading
                return Environment.GetEnvironmentVariable("DREMIO_SERVER_AUTH") ?? _dremioServerAuth;
            }
            set
            {
                // custom logic when setting
                Console.WriteLine($"Setting Name to {value}");
                if (!string.IsNullOrWhiteSpace(value))
                {
                    _dremioServerAuth = value;
                }
                else
                {
                    throw new ArgumentException("DremioServer cannot be empty");
                }
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
