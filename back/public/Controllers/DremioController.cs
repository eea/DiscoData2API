using DiscoData2API.Services;
using DiscoData2API.Model;
using Microsoft.AspNetCore.Mvc;
using System.Text.RegularExpressions;
using System.Text.Json;
using DiscoData2API.Misc;
using DiscoData2API.Class;

namespace DiscoData2API.Controllers
{
      [ApiController]
      [Route("api/[controller]")]
      public class DremioController(ILogger<DremioController> logger, DremioService dremioService) : ControllerBase
      {
            private readonly ILogger<DremioController> _logger = logger;
            private readonly int _defaultLimit = dremioService._limit;
            private readonly int _timeout = dremioService._timeout;

            [HttpPost("query-execution")]
            [Produces("application/json")]
            public async Task<IActionResult> ExecuteWiseQuery([FromBody] WiseQueryRequest request, CancellationToken cts)
            {
                  try
                  {
                        if (string.IsNullOrWhiteSpace(request.Query))
                        {
                              return BadRequest(new { error = "Query cannot be empty" });
                        }

                        var result = await dremioService.ExecuteWiseQuery(request.Query, cts);
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
