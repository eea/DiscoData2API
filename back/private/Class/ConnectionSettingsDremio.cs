namespace DiscoData2API_Priv.Class
{
    public class ConnectionSettingsDremio
    {
        public string? Username { get; set; }
        public string? Password { get; set; }
        public string? DremioServer { get; set; }
        public string? DremioServerAuth { get; set; }
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
