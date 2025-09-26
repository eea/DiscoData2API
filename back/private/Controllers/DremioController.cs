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
    [ApiExplorerSettings(IgnoreApi = true)]
    public class DremioController(ILogger<ViewController> logger, DremioService dremioService) : ControllerBase
    {
        private readonly int _defaultLimit = dremioService._limit;
        private readonly int _timeout = dremioService._timeout;
        private readonly ILogger<ViewController> _logger;


        [HttpGet("GetSchema")]
        public async Task<ActionResult<string>> GetSchema(string origin)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var query = "SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like '%" + origin + "%'";
                var result = await dremioService.ExecuteQuery(query, cts.Token);
                result = FixMalformedJson(result);

                List<DremioRawSchema> rawSchemaList = JsonSerializer.Deserialize<List<DremioRawSchema>>(result) ?? new List<DremioRawSchema>();

                if (rawSchemaList == null)
                {
                    return NotFound("No schema found.");
                }

                List<DremioSchema> schemaList = new List<DremioSchema>();
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
                    schema.SchemaName = schemaPathItem[schemaPathItem.Length - 2];
                }

                return Ok(schemaList);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in GetTable");
                return StatusCode(500, ex.Message);
            }
        }

        [HttpGet("GetTable/{schema}")]
        public async Task<List<DremioTable>> GetTable(string? schema)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA = '" + schema + "'";
                var result = await dremioService.ExecuteQuery(query, cts.Token);
                var rawTableList = JsonSerializer.Deserialize<List<DremioRawTable>>(result) ?? new List<DremioRawTable>();

                List<DremioTable> tableList = new List<DremioTable>();
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
                logger.LogError(ex, "Error in GetTableDetail");
                return new List<DremioTable>();
            }
        }

        [HttpGet("GetColumn/{schema}/{table}")]
        public async Task<List<DremioColumn>> GetColumn(string? schema, string? table)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var query = "SELECT * FROM INFORMATION_SCHEMA.\"COLUMNS\" WHERE TABLE_SCHEMA = '" + schema + "' and TABLE_NAME = '" + table + "'";
                var result = await dremioService.ExecuteQuery(query, cts.Token);
                var columns = JsonSerializer.Deserialize<List<DremioColumn>>(result) ?? new List<DremioColumn>();
                return columns;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in GetTableDetail");
                return new List<DremioColumn>();
            }
        }

        [HttpPost("TestQuery")]
        public async Task<ActionResult<string>> TestQuery([FromBody] DremioJob request)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                var result = await dremioService.ExecuteQuery(request.Query, cts.Token);
                return result;
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (SQLFormattingException ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
            catch (Exception ex)
            {
                logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;
                return StatusCode(StatusCodes.Status400BadRequest, errmsg);
            }
        }
        #region Helper Methods
        private string FixMalformedJson(string json)
        {
            // Ensure there is a comma between JSON objects
            json = Regex.Replace(json, @"}(\s*){", "},{");

            return json;
        }
        #endregion
    }
}
