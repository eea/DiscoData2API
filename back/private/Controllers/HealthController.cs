using DiscoData2API_Priv.Services;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace DiscoData2API_Priv.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
     [ApiExplorerSettings(IgnoreApi = true)]
    public class HealthController : ControllerBase
    {
        private readonly ILogger<HealthController> _logger;
        private readonly FlightClientPool _flightClientPool;
        private readonly QueryThrottlingService _throttlingService;

        public HealthController(ILogger<HealthController> logger, FlightClientPool flightClientPool, QueryThrottlingService throttlingService)
        {
            _logger = logger;
            _flightClientPool = flightClientPool;
            _throttlingService = throttlingService;
        }

        /// <summary>
        /// Get system health metrics for monitoring heavy usage
        /// </summary>
        /// <returns>System health information</returns>
        [HttpGet]
        public ActionResult<object> GetHealth()
        {
            try
            {
                var process = Process.GetCurrentProcess();
                var memoryMB = process.WorkingSet64 / (1024 * 1024);
                var gcMemoryMB = GC.GetTotalMemory(false) / (1024 * 1024);

                var health = new
                {
                    Status = "healthy",
                    Timestamp = DateTime.UtcNow,
                    Memory = new
                    {
                        WorkingSetMB = memoryMB,
                        GCMemoryMB = gcMemoryMB,
                        Gen0Collections = GC.CollectionCount(0),
                        Gen1Collections = GC.CollectionCount(1),
                        Gen2Collections = GC.CollectionCount(2)
                    },
                    FlightClientPool = new
                    {
                        AvailableClients = _flightClientPool.AvailableClients,
                        TotalClients = _flightClientPool.TotalClients
                    },
                    QueryThrottling = new
                    {
                        ActiveQueries = _throttlingService.ActiveQueryCount,
                        AvailableSlots = _throttlingService.AvailableSlots
                    },
                    Performance = new
                    {
                        ProcessorTimeMs = process.TotalProcessorTime.TotalMilliseconds,
                        ThreadCount = process.Threads.Count,
                        HandleCount = process.HandleCount
                    }
                };

                // Add warning flags for concerning metrics
                var warnings = new List<string>();

                if (memoryMB > 2048) // > 2GB
                    warnings.Add("High memory usage");

                if (_throttlingService.AvailableSlots == 0)
                    warnings.Add("Query queue full");

                if (_flightClientPool.AvailableClients == 0 && _flightClientPool.TotalClients > 5)
                    warnings.Add("Low available connections");

                if (warnings.Any())
                {
                    return Ok(new { health, Warnings = warnings });
                }

                return Ok(health);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting health metrics");
                return StatusCode(500, new { Status = "unhealthy", Error = ex.Message });
            }
        }

        /// <summary>
        /// Trigger garbage collection (for emergency memory management)
        /// </summary>
        [HttpPost("gc")]
        public ActionResult ForceGarbageCollection()
        {
            try
            {
                var beforeMB = GC.GetTotalMemory(false) / (1024 * 1024);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                var afterMB = GC.GetTotalMemory(false) / (1024 * 1024);

                _logger.LogInformation($"Manual GC triggered: {beforeMB}MB -> {afterMB}MB (freed {beforeMB - afterMB}MB)");

                return Ok(new
                {
                    Success = true,
                    BeforeMemoryMB = beforeMB,
                    AfterMemoryMB = afterMB,
                    FreedMemoryMB = beforeMB - afterMB
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during manual garbage collection");
                return StatusCode(500, new { Success = false, Error = ex.Message });
            }
        }

        /// <summary>
        /// Get connection pool statistics
        /// </summary>
        [HttpGet("connections")]
        public ActionResult GetConnectionPoolStats()
        {
            return Ok(new
            {
                FlightClientPool = new
                {
                    AvailableClients = _flightClientPool.AvailableClients,
                    TotalClients = _flightClientPool.TotalClients,
                    PoolUtilization = _flightClientPool.TotalClients > 0
                        ? (double)(_flightClientPool.TotalClients - _flightClientPool.AvailableClients) / _flightClientPool.TotalClients * 100
                        : 0
                },
                QueryThrottling = new
                {
                    ActiveQueries = _throttlingService.ActiveQueryCount,
                    AvailableSlots = _throttlingService.AvailableSlots,
                    QueueUtilization = _throttlingService.AvailableSlots > 0
                        ? (double)_throttlingService.ActiveQueryCount / (_throttlingService.ActiveQueryCount + _throttlingService.AvailableSlots) * 100
                        : 100
                }
            });
        }
    }
}