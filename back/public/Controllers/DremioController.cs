using DiscoData2API.Services;
using DiscoData2API.Model;
using Microsoft.AspNetCore.Mvc;

namespace DiscoData2API.Controllers
{
      [ApiController]
      [Route("api/[controller]")]
      public class DremioController(ILogger<DremioController> logger, DremioService dremioService) : ControllerBase
      {
            private readonly ILogger<DremioController> _logger = logger;
            private readonly DremioService _dremioService = dremioService;
            private readonly int _timeout = dremioService._timeout;

            [HttpGet("health")]
            [Produces("application/json")]
            public async Task<IActionResult> HealthCheck()
            {
                var (reachable, message) = await _dremioService.CheckHealth();
                var status = new { dremio = reachable ? "ok" : "unreachable", message };
                return reachable ? Ok(status) : StatusCode(503, status);
            }

            /// <summary>Executes a SQL query and returns results in Dremio's native format: <c>columns</c> array + <c>rows</c> array with <c>{ "v": value }</c> cells.</summary>
            [HttpPost("ExecuteRawQuery")]
            [Produces("application/json")]
            [ApiExplorerSettings(IgnoreApi = false)]
            public async Task<IActionResult> ExecuteRawQuery([FromBody] WiseQueryRequest request)
            {
                  using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
                  try
                  {
                        if (string.IsNullOrWhiteSpace(request.Query))
                        {
                              return BadRequest(new { error = "Query cannot be empty" });
                        }

                        var result = await _dremioService.ExecuteRawQuery(request.Query, cts.Token);
                        return Ok(result);
                  }
                  catch (Exception ex)
                  {
                        _logger.LogError(ex, "Error executing WISE query");
                        return StatusCode(500, new { error = ex.Message });
                  }
            }

            // Request model for WiseQuery endpoint
            public class WiseQueryRequest
            {
                  public required string Query { get; set; }
            }

      }
}
