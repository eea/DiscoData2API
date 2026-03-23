using DiscoData2API_Priv.Services;
using DiscoData2API_Priv.Model;
using Microsoft.AspNetCore.Mvc;
using System.Text.RegularExpressions;
using System.Text.Json;
using DiscoData2API_Priv.Misc;
using DiscoData2API_Priv.Class;

namespace DiscoData2API_Priv.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [ApiExplorerSettings(IgnoreApi = false)]
    public class DremioController(ILogger<DremioController> logger, DremioService dremioService) : ControllerBase
    {
        private readonly ILogger<DremioController> _logger = logger;
        private readonly int _defaultLimit = dremioService._limit;
        private readonly int _timeout = dremioService._timeout;

        /// <summary>Returns Dremio connection status.</summary>
        [HttpGet("health")]
        [Produces("application/json")]
        public async Task<IActionResult> HealthCheck()
        {
            var (reachable, message) = await dremioService.CheckHealth();
            var status = new { dremio = reachable ? "ok" : "unreachable", message };
            return reachable ? Ok(status) : StatusCode(503, status);
        }

        /// <summary>Returns schemas matching the given origin filter.</summary>
        /// <param name="origin">Partial schema name to filter on.</param>
        [HttpGet("GetSchema")]
        public async Task<ActionResult<string>> GetSchema(string origin)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var query = "SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like '%" + origin + "%'";
                var result = await dremioService.ExecuteQuery(query, cts.Token);
                result = FixMalformedJson(result);

                List<DremioRawSchema> rawSchemaList = JsonSerializer.Deserialize<List<DremioRawSchema>>(result) ?? [];

                if (rawSchemaList == null)
                {
                    return NotFound("No schema found.");
                }

                List<DremioSchema> schemaList = [];
                foreach (var item in rawSchemaList)
                {
                    schemaList.Add(new DremioSchema()
                    {
                        SchemaName = item.SchemaName,
                        Schema = item.TABLE_SCHEMA
                    });
                }

                foreach (var schema in schemaList)
                {
                    var schemaPathItem = schema.Schema.Split('.');
                    schema.SchemaName = schemaPathItem[^2];
                }

                return Ok(schemaList);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetTable");
                return StatusCode(500, ex.Message);
            }
        }

        /// <summary>Returns all tables in the given schema.</summary>
        /// <param name="schema">Full schema path (e.g. <c>my_source.my_folder</c>).</param>
        [HttpGet("GetTable/{schema}")]
        public async Task<List<DremioTable>> GetTable(string? schema)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA = '" + schema + "'";
                var result = await dremioService.ExecuteQuery(query, cts.Token);
                var rawTableList = JsonSerializer.Deserialize<List<DremioRawTable>>(result) ?? [];

                List<DremioTable> tableList = [];
                foreach (var item in rawTableList)
                {
                    tableList.Add(new DremioTable()
                    {
                        TableName = item.TABLE_NAME
                    });
                }


                return tableList;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetTableDetail");
                return new List<DremioTable>();
            }
        }

        /// <summary>Returns all columns for a given schema and table.</summary>
        /// <param name="schema">Full schema path.</param>
        /// <param name="table">Table name.</param>
        [HttpGet("GetColumn/{schema}/{table}")]
        public async Task<List<DremioColumn>> GetColumn(string? schema, string? table)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var query = "SELECT * FROM INFORMATION_SCHEMA.\"COLUMNS\" WHERE TABLE_SCHEMA = '" + schema + "' and TABLE_NAME = '" + table + "'";
                var result = await dremioService.ExecuteQuery(query, cts.Token);
                var columns = JsonSerializer.Deserialize<List<DremioColumn>>(result) ?? [];
                return columns;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetTableDetail");
                return new List<DremioColumn>();
            }
        }

        /// <summary>Executes a SQL query and returns results as a flat JSON array (one object per row). Suitable for datagrid binding.</summary>
        /// <remarks>Subject to the configured query timeout.</remarks>
        [HttpPost("ExecuteQuery")]
        [ProducesResponseType(typeof(string), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status408RequestTimeout)]
        public async Task<ActionResult<string>> ExecuteQuery([FromBody] DremioJob request)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var result = await dremioService.ExecuteQuery(request.Query, cts.Token);
                return result;
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (SQLFormattingException ex)
            {
                _logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;
                return StatusCode(StatusCodes.Status400BadRequest, errmsg);
            }
        }

        /// <summary>Executes a SQL query and returns results in Dremio's native format: <c>columns</c> array + <c>rows</c> array with <c>{ "v": value }</c> cells.</summary>
        [HttpPost("ExecuteRawQuery")]
        [Produces("application/json")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> ExecuteRawQuery([FromBody] QueryRequest request, CancellationToken cts)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.Query))
                {
                    return BadRequest(new { error = "Query cannot be empty" });
                }

                var result = await dremioService.ExecuteRawQuery(request.Query, cts);
                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing WISE query");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        // Request model (if not already defined)
        public class QueryRequest
        {
            public string Query { get; set; }
        }

        #region Helper Methods
        private static string FixMalformedJson(string json)
        {
            // Ensure there is a comma between JSON objects
            json = Regex.Replace(json, @"}(\s*){", "},{");

            return json;
        }
        #endregion
    }
}
