using DiscoData2API_Priv.Services;
using DiscoData2API_Priv.Class;
using DiscoData2API_Priv.Model;
using Microsoft.AspNetCore.Mvc;
using DiscoData2API_Priv.Misc;
using System.Text.RegularExpressions;
using System.Text.Json;

namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class QueryController : ControllerBase
    {
        private readonly ILogger<QueryController> _logger;
        private readonly MongoService _mongoService;
        private readonly DremioService _dremioService;
        private readonly int _defaultLimit;
        private readonly int _timeout;

        public QueryController(ILogger<QueryController> logger, MongoService mongoService, DremioService dremioService)
        {
            _logger = logger;
            _mongoService = mongoService;
            _dremioService = dremioService;
            _defaultLimit = dremioService._limit;
            _timeout = dremioService._timeout;
        }

        /// <summary>
        /// Create a query (MongoDB)
        /// </summary>
        /// <param name="request"></param>
        /// <returns>Return the Json document saved</returns>
        [HttpPost("CreateQuery")]
        public async Task<ActionResult<MongoDocument>> CreateQuery([FromBody] MongoDocument request)
        {
            if (string.IsNullOrEmpty(request.Query) || !SQLExtensions.ValidateSQL(request.Query))
            {
                _logger.LogWarning("SQL query contains unsafe keywords.");
                return BadRequest("SQL query contains unsafe keywords.");
            }

            return await _mongoService.CreateAsync(new MongoDocument()
            {
                Query = request.Query,
                Name = request.Name,
                UserAdded = request.UserAdded,
                Version = request.Version,
                Fields = extractFields(request.Query).Result,
                IsActive = true,
                Date = DateTime.Now
            });
        }

        /// <summary>
        /// Read a query (MongoDB)
        /// </summary>
        /// <param name="id">The Id of the query to read</param>
        /// <returns>Return a MongoDocument</returns>
        [HttpGet("ReadQuery/{id}")]
        public async Task<ActionResult<MongoDocument>> ReadQuery(string id)
        {
            return await _mongoService.ReadAsync(id);
        }

        /// <summary>
        /// Update a query (MongoDB)
        /// </summary>
        /// <param name="id">The Id of the query to update</param>
        /// <param name="request"></param>
        /// <returns>Return the updated MongoDocument</returns>
        [HttpPost("UpdateQuery/{id}")]
        public async Task<ActionResult<MongoDocument>> UpdateQuery(string id, [FromBody] MongoDocument request)
        {
            if (request.Query == null || !SQLExtensions.ValidateSQL(request.Query))
            {
                _logger.LogWarning("SQL query contains unsafe keywords.");
                return BadRequest("SQL query contains unsafe keywords.");
            }

            //we update the fields in case the query changed
            request.Fields = extractFields(request.Query).Result;

            var updatedDocument = await _mongoService.UpdateAsync(id, request);
            if (updatedDocument == null)
            {
                _logger.LogWarning($"Document with id {id} could not be updated.");
                return NotFound($"Document with id {id} not found.");
            }

            return Ok(updatedDocument);
        }

        /// <summary>
        /// Delete a query (MongoDB)
        /// </summary>
        /// <param name="id"></param>
        /// <returns>Return True if deleted</returns>
        [HttpDelete("DeleteQuery/{id}")]
        public async Task<ActionResult<bool>> DeleteQuery(string id)
        {
            return await _mongoService.DeleteAsync(id);
        }

        /// <summary>
        /// Get catalog of queries (MongoDB)
        /// </summary>
        /// <returns>Return a list of MongoDocument class</returns>
        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoDocument>>> GetMongoCatalog([FromQuery]string userAdded)
        {
            if (string.IsNullOrEmpty(userAdded))
            {
                return await _mongoService.GetAllAsync();
            }

            return await _mongoService.GetAllByUserAsync(userAdded);
        }

        /// <summary>
        /// Execute a query and return JSON result
        /// </summary>
        /// <param name="id">The mongoDb query ID</param>
        /// <param name="request">The JSON body of the httpRequest</param>
        /// <returns></returns>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /api/query/672b84ef75e2d0b792658f24
        ///     {
        ///     "fields": ["column1", "column2"],
        ///     "limit": 100
        ///     }
        ///         
        /// </remarks>
        /// <response code="201">Returns the newly created item</response>
        /// <response code="400">If the item is null</response>
        [HttpPost("{id}")]
        public async Task<ActionResult<string>> ExecuteQuery(string id, [FromBody] QueryRequest request)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                MongoDocument? mongoDoc = await _mongoService.ReadAsync(id);

                if (mongoDoc == null)
                {
                    _logger.LogError($"Query with id {id} not found");
                    return NotFound();
                }
                else
                {
                    mongoDoc.Query = UpdateQueryString(mongoDoc.Query, request.Fields, request.Limit, request.Filters);
                }

                var result = await _dremioService.ExecuteQuery(mongoDoc.Query, cts.Token);

                return result;
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                return ex.Message;
            }
        }

        #region Extract fields from query

        private async Task<List<Field>> extractFields(string? query)
        {
            List<Field> fieldsList = new List<Field>();

            try
            {
                // Extract all table names from the query
                List<string> tables = ExtractTableNames(query);

                // Get the columns of each table
                foreach (var table in tables.Distinct())
                {
                    var queryColumns = $"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}';";
                    using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
                    var result = await _dremioService.ExecuteQuery(queryColumns, cts.Token);
                    var columns = JsonSerializer.Deserialize<List<List<DremioColumn>>>(result);

                    if (columns != null)
                    {
                        foreach (var column in columns[0])
                        {
                            fieldsList.Add(new Field()
                            {
                                Name = column.COLUMN_NAME,
                                Type = column.DATA_TYPE,
                                IsNullable = column.IS_NULLABLE == "YES",
                                ColumnSize = column.COLUMN_SIZE.ToString()
                            });
                        }
                    }
                }

                return fieldsList;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                return fieldsList;
            }
        }
        private List<string> ExtractTableNames(string query)
        {
            List<string> tableNames = new List<string>();

            // Regular expression to match table names
            string pattern = @"(?:FROM|JOIN)\s+((?:\""[^\""]+\""|\[[^\]]+\]|\w+)(?:\.(?:\""[^\""]+\""|\[[^\]]+\]|\w+))*)";
            Regex regex = new Regex(pattern, RegexOptions.IgnoreCase);

            MatchCollection matches = regex.Matches(query);
            foreach (Match match in matches)
            {
                if (match.Groups.Count > 1)
                {
                    string tableName = match.Groups[1].Value.LastIndexOf(".") > 0 ? match.Groups[1].Value.Split('.').Last() : match.Groups[1].Value;
                    tableNames.Add(tableName);
                }
            }

            return tableNames;
        }

        #endregion

        #region helper

        /// <summary>
        /// Update the query string with fields and limit parameters
        /// </summary>
        /// <param name="query"></param>
        /// <param name="fields"></param>
        /// <param name="limit"></param>
        /// <param name="filters"></param>
        /// <returns></returns>
        private string UpdateQueryString(string query, string[]? fields, int? limit, string[]? filters)
        {
            // Update fields returned by query
            fields = fields != null && fields.Length > 0 ? fields : new string[] { "*" };
            query = query.Replace("*", string.Join(",", fields));

            // Add filters to query
            if (filters != null && filters.Length > 0)
            {
                // Concatenate filters directly without additional "AND"
                string filterClause = string.Join(" ", filters); // Maintain operators and conditions as provided

                // Ensure WHERE clause is correctly placed
                if (query.Contains("WHERE", StringComparison.OrdinalIgnoreCase))
                {
                    query = query.TrimEnd(); // Remove any trailing spaces
                    query += $" AND {filterClause}";
                }
                else
                {
                    query += $" WHERE {filterClause}";
                }
            }

            // Ensure LIMIT is always at the end
            limit = limit.HasValue && limit != 0 ? limit.Value : _defaultLimit;

            // Remove any existing LIMIT clause and append a new one
            query = System.Text.RegularExpressions.Regex.Replace(query, @"LIMIT\s+\d+", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            query += $" LIMIT {limit}";

            if (!SQLExtensions.ValidateSQL(query))
            {
                _logger.LogWarning("SQL query contains unsafe keywords.");
                throw new Exception("SQL query contains unsafe keywords.");
            }
            return query;
        }

        #endregion
    }
}