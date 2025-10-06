using Apache.Arrow.Flight.Client;
using DiscoData2API.Class;
using Grpc.Net.Client;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;

namespace DiscoData2API.Services
{
    public class FlightClientPool : IDisposable
    {
        private readonly ILogger<FlightClientPool> _logger;
        private readonly string _dremioServer;
        private readonly ConcurrentQueue<FlightClient> _availableClients;
        private readonly ConcurrentDictionary<FlightClient, DateTime> _clientLastUsed;
        private readonly int _maxPoolSize;
        private readonly int _minPoolSize;
        private readonly TimeSpan _clientTimeout;
        private readonly Timer _cleanupTimer;
        private readonly object _lock = new object();
        private bool _disposed = false;

        public FlightClientPool(IOptions<ConnectionSettingsDremio> dremioSettings, ILogger<FlightClientPool> logger)
        {
            _logger = logger;
            _dremioServer = dremioSettings.Value.DremioServer ?? throw new ArgumentNullException("DremioServer");
            _availableClients = new ConcurrentQueue<FlightClient>();
            _clientLastUsed = new ConcurrentDictionary<FlightClient, DateTime>();

            // Pool configuration - can be moved to settings
            _maxPoolSize = 20;
            _minPoolSize = 2;
            _clientTimeout = TimeSpan.FromMinutes(10);

            // Initialize minimum pool size
            InitializePool();

            // Start cleanup timer to remove idle connections
            _cleanupTimer = new Timer(CleanupIdleClients, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        private void InitializePool()
        {
            for (int i = 0; i < _minPoolSize; i++)
            {
                var client = CreateFlightClient();
                _availableClients.Enqueue(client);
                _clientLastUsed[client] = DateTime.UtcNow;
            }
            _logger.LogInformation($"Initialized FlightClient pool with {_minPoolSize} clients");
        }

        public FlightClient GetClient()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(FlightClientPool));

            FlightClient? client = null;

            // Try to get an available client
            if (_availableClients.TryDequeue(out client))
            {
                _clientLastUsed[client] = DateTime.UtcNow;
                _logger.LogDebug("Reused existing FlightClient from pool");
                return client;
            }

            // Create new client if pool is not at max capacity
            lock (_lock)
            {
                var totalClients = _clientLastUsed.Count;
                if (totalClients < _maxPoolSize)
                {
                    client = CreateFlightClient();
                    _clientLastUsed[client] = DateTime.UtcNow;
                    _logger.LogDebug($"Created new FlightClient, pool size: {totalClients + 1}");
                    return client;
                }
            }

            // If pool is full, wait a bit and try again
            _logger.LogWarning("FlightClient pool is full, waiting for available client");
            Thread.Sleep(100);

            // Try one more time
            if (_availableClients.TryDequeue(out client))
            {
                _clientLastUsed[client] = DateTime.UtcNow;
                return client;
            }

            // As last resort, create a temporary client (not pooled)
            _logger.LogWarning("Creating temporary FlightClient due to pool exhaustion");
            return CreateFlightClient();
        }

        public void ReturnClient(FlightClient client)
        {
            if (_disposed || client == null)
                return;

            // Check if this client is still tracked
            if (_clientLastUsed.ContainsKey(client))
            {
                _clientLastUsed[client] = DateTime.UtcNow;
                _availableClients.Enqueue(client);
                _logger.LogDebug("Returned FlightClient to pool");
            }
            else
            {
                // This is a temporary client, dispose it
                try
                {
                    // FlightClient doesn't implement IDisposable directly
                    // The underlying GrpcChannel will be disposed by GC
                    _logger.LogDebug("Disposed temporary FlightClient");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error disposing temporary FlightClient: {ex.Message}");
                }
            }
        }

        private FlightClient CreateFlightClient()
        {
            var httpHandler = new HttpClientHandler();
            httpHandler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
            var channel = GrpcChannel.ForAddress(_dremioServer, new GrpcChannelOptions { HttpHandler = httpHandler });
            return new FlightClient(channel);
        }

        private void CleanupIdleClients(object? state)
        {
            if (_disposed)
                return;

            var cutoffTime = DateTime.UtcNow - _clientTimeout;
            var clientsToRemove = new List<FlightClient>();

            // Find idle clients
            foreach (var kvp in _clientLastUsed)
            {
                if (kvp.Value < cutoffTime)
                {
                    clientsToRemove.Add(kvp.Key);
                }
            }

            // Remove idle clients but maintain minimum pool size
            var currentPoolSize = _clientLastUsed.Count;
            var maxToRemove = Math.Max(0, currentPoolSize - _minPoolSize);
            var actualRemoveCount = Math.Min(clientsToRemove.Count, maxToRemove);

            for (int i = 0; i < actualRemoveCount; i++)
            {
                var client = clientsToRemove[i];
                if (_clientLastUsed.TryRemove(client, out _))
                {
                    try
                    {
                        // FlightClient doesn't implement IDisposable directly
                    // The underlying GrpcChannel will be disposed by GC
                        _logger.LogDebug("Disposed idle FlightClient");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Error disposing idle FlightClient: {ex.Message}");
                    }
                }
            }

            if (actualRemoveCount > 0)
            {
                _logger.LogInformation($"Cleaned up {actualRemoveCount} idle FlightClients, pool size: {_clientLastUsed.Count}");
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _cleanupTimer?.Dispose();

            // Dispose all clients
            while (_availableClients.TryDequeue(out var client))
            {
                try
                {
                    // FlightClient doesn't implement IDisposable directly
                    // The underlying GrpcChannel will be disposed by GC
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error disposing FlightClient during shutdown: {ex.Message}");
                }
            }

            foreach (var kvp in _clientLastUsed)
            {
                try
                {
                    // FlightClient doesn't implement IDisposable directly
                    // The underlying GrpcChannel will be disposed by GC
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error disposing FlightClient during shutdown: {ex.Message}");
                }
            }

            _clientLastUsed.Clear();
            _logger.LogInformation("FlightClientPool disposed");
        }

        public int AvailableClients => _availableClients.Count;
        public int TotalClients => _clientLastUsed.Count;
    }
}