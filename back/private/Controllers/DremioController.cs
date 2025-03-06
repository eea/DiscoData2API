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
    public class DremioController(ILogger<ViewController> logger, MongoDatasetService mongoDatasetService, DremioService dremioService) : ControllerBase
    {
        private readonly int _defaultLimit = dremioService._limit;
        private readonly int _timeout = dremioService._timeout;
        private readonly ILogger<ViewController> _logger;


        /*
        [HttpGet("UpdateSchema")]
        public async Task<ActionResult<string>> UpdateSchema()
        {
            var datasets=  await mongoDatasetService.GetAllAsync();
            foreach (var dt in datasets)
            {
                if (dt.Tables == null)
                    continue;

                if (dt.Tables.Any())
                {
                    try
                    {
                        foreach (var table in dt.Tables)
                        {
                            string query =  string.Format("select * from {0} limit 1", table.DremioRoute);
                            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
                            try
                            {
                                List<Field> fields = await ExtractFieldsFromQuery(query);
                                List<DatasetTableField>? table_fields = new List<DatasetTableField>();
                                foreach (var field in fields)
                                {
                                    if (field.Name != "DH_ID")
                                    {
                                        table_fields.Add(new DatasetTableField
                                        {
                                            Name = field.Name,
                                            Type = field.Type
                                        });
                                    }
                                }
                                table.Fields = table_fields;
                            }
                            catch 
                            {

                            }
                        }

                    }
                    catch (Exception ex) 
                    {
                        _logger.LogError(ex, $"Error in update schema {dt.ID} ex:{ex.Message}" );
                    }
                
                    
                }

                await mongoDatasetService.Update(dt);
                
            }

            return "OK";
        }
        */

        private async Task<List<Field>> ExtractFieldsFromQuery(string? query)
        {
            try
            {
                //var temp_table_name = string.Format("\"Local S3\".\"datahub-pre-01\".discodata.\"temp_{0}\"", System.Guid.NewGuid().ToString());
                using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
                var queryColumnsFlight = string.Format(@" select * from ({0} ) limit 1;", query);
                return await dremioService.GetSchema(queryColumnsFlight, cts.Token);
            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);

                throw; // new Exception("Invalid query");
            }
        }

        [HttpGet("GetSchema")]
        public async Task<ActionResult<string>> GetSchema(string origin)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                // var query = "SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like '%datasets%'";
                var query = "SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA like '%" + origin + "%'";
                var result = await dremioService.ExecuteQuery(query, cts.Token);
                result = FixMalformedJson(result);

                List<DremioSchema> schemaList = JsonSerializer.Deserialize<List<DremioSchema>>(result) ?? new List<DremioSchema>();

                if (schemaList == null)
                {
                    return NotFound("No schema found.");
                }

                foreach (var schema in schemaList)
                {
                    var schemaPathItem = schema.TableSchema.Split('.');
                    schema.SchemaName = schemaPathItem[schemaPathItem.Length - 2];
                    // table.Columns = await GetColumn(table.TableSchema);   //takes too much time to get all columns
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
                var tables = JsonSerializer.Deserialize<List<DremioTable>>(result) ?? new List<DremioTable>();
                return tables;
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
