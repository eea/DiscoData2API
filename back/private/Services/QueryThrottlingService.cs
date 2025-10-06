using DiscoData2API_Priv.Class;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;

namespace DiscoData2API_Priv.Services
{
    public class QueryThrottlingService
    {
        private readonly ILogger<QueryThrottlingService> _logger;
        private readonly SemaphoreSlim _concurrencyLimit;
        private readonly ConcurrentDictionary<string, DateTime> _activeQueries;
        private readonly int _maxConcurrentQueries;
        private readonly bool _enableThrottling;
        private readonly int _throttleDelayMs;
        private readonly Timer _cleanupTimer;

        public QueryThrottlingService(IOptions<ConnectionSettingsDremio> dremioSettings, ILogger<QueryThrottlingService> logger)
        {
            _logger = logger;
            _maxConcurrentQueries = dremioSettings.Value.MaxConcurrentQueries;
            _enableThrottling = dremioSettings.Value.EnableQueryThrottling;
            _throttleDelayMs = dremioSettings.Value.ThrottleDelayMs;

            _concurrencyLimit = new SemaphoreSlim(_maxConcurrentQueries, _maxConcurrentQueries);
            _activeQueries = new ConcurrentDictionary<string, DateTime>();

            // Cleanup timer to remove stale query entries
            _cleanupTimer = new Timer(CleanupStaleQueries, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            _logger.LogInformation($"QueryThrottlingService initialized - MaxConcurrent: {_maxConcurrentQueries}, ThrottlingEnabled: {_enableThrottling}");
        }

        /// <summary>
        /// Acquires permission to execute a query, blocking if necessary
        /// </summary>
        /// <param name="queryId">Unique identifier for the query</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Query execution token that must be disposed when query completes</returns>
        public async Task<QueryExecutionToken> AcquireQuerySlotAsync(string queryId, CancellationToken cancellationToken = default)
        {
            if (!_enableThrottling)
            {
                return new QueryExecutionToken(queryId, this, false);
            }

            var startTime = DateTime.UtcNow;
            _logger.LogDebug($"Query {queryId} requesting execution slot, active queries: {_activeQueries.Count}");

            try
            {
                // Wait for available slot
                await _concurrencyLimit.WaitAsync(cancellationToken);

                // Add to active queries
                _activeQueries[queryId] = DateTime.UtcNow;

                var waitTime = DateTime.UtcNow - startTime;
                if (waitTime.TotalMilliseconds > 100)
                {
                    _logger.LogInformation($"Query {queryId} acquired slot after {waitTime.TotalMilliseconds:F0}ms wait");
                }

                // Add small delay to prevent overwhelming
                if (_throttleDelayMs > 0)
                {
                    await Task.Delay(_throttleDelayMs, cancellationToken);
                }

                return new QueryExecutionToken(queryId, this, true);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning($"Query {queryId} acquisition cancelled after {(DateTime.UtcNow - startTime).TotalMilliseconds:F0}ms");
                throw;
            }
        }

        /// <summary>
        /// Releases a query execution slot
        /// </summary>
        /// <param name="queryId">Query identifier</param>
        /// <param name="wasThrottled">Whether the query was actually throttled</param>
        internal void ReleaseQuerySlot(string queryId, bool wasThrottled)
        {
            if (wasThrottled)
            {
                _activeQueries.TryRemove(queryId, out _);
                _concurrencyLimit.Release();
                _logger.LogDebug($"Query {queryId} released execution slot, active queries: {_activeQueries.Count}");
            }
        }

        private void CleanupStaleQueries(object? state)
        {
            var cutoffTime = DateTime.UtcNow.AddMinutes(-30); // Remove queries older than 30 minutes
            var staleQueries = _activeQueries.Where(kvp => kvp.Value < cutoffTime).ToList();

            foreach (var staleQuery in staleQueries)
            {
                if (_activeQueries.TryRemove(staleQuery.Key, out _))
                {
                    _logger.LogWarning($"Removed stale query {staleQuery.Key} from active tracking");
                    try
                    {
                        _concurrencyLimit.Release();
                    }
                    catch (SemaphoreFullException)
                    {
                        // Semaphore is already at max capacity
                    }
                }
            }

            if (staleQueries.Count > 0)
            {
                _logger.LogInformation($"Cleaned up {staleQueries.Count} stale queries");
            }
        }

        public int ActiveQueryCount => _activeQueries.Count;
        public int AvailableSlots => _concurrencyLimit.CurrentCount;

        public void Dispose()
        {
            _cleanupTimer?.Dispose();
            _concurrencyLimit?.Dispose();
        }
    }

    /// <summary>
    /// Token representing a query execution slot that must be disposed when the query completes
    /// </summary>
    public class QueryExecutionToken : IDisposable
    {
        private readonly string _queryId;
        private readonly QueryThrottlingService _service;
        private readonly bool _wasThrottled;
        private bool _disposed = false;

        internal QueryExecutionToken(string queryId, QueryThrottlingService service, bool wasThrottled)
        {
            _queryId = queryId;
            _service = service;
            _wasThrottled = wasThrottled;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _service.ReleaseQuerySlot(_queryId, _wasThrottled);
                _disposed = true;
            }
        }
    }
}