// ────────────────────────────────────────────────────────────────────────
// Entire controller commented out: raw Dremio query execution is no
// longer exposed. The DuckDB sidecar handles all query execution.
// ────────────────────────────────────────────────────────────────────────

// using DiscoData2API.Services;
// using DiscoData2API.Model;
// using Microsoft.AspNetCore.Mvc;
//
// namespace DiscoData2API.Controllers
// {
//     [ApiController]
//     [Route("api/dremio")]
//     public class DremioController(ILogger<DremioController> logger, DremioService dremioService) : ControllerBase
//     {
//         [HttpPost("execute-raw-query")]
//         [ApiExplorerSettings(IgnoreApi = true)]
//         public async Task<IActionResult> ExecuteRawQuery([FromBody] WiseQueryRequest request) { ... }
//     }
// }
