using DiscoData2API.Services;
using DiscoData2API.Class;
using DiscoData2API.Model;
using DiscoData2API.Misc;
using Microsoft.AspNetCore.Mvc;


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
        /// Get catalog of pre-processed views
        /// </summary>
        /// <returns>Returns a list of pre-processed views</returns>
        /// 
        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoPublicDocument>>> GetMongoCatalog()
        {
            return await  _mongoService.GetAllAsync();
        }

        /// <summary>
        /// Executes a query and returns a JSON with the results
        /// </summary>
        /// <param name="id">The query ID</param>
        /// <param name="request">The JSON body of the request</param>
        /// <returns></returns>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /api/query/672b84ef75e2d0b792658f24
        ///     {
        ///     "fields": ["column1", "column2"],
        ///     "filters": ["column1 = 'value1'", "column2 = 'value2'"],
        ///     "limit": 100,
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
                MongoDocument? mongoDoc = await _mongoService.GetById(id);

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

        #region helper

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

            if(!SqlHelper.IsSafeSql(query))
            {
                _logger.LogWarning("SQL query contains unsafe keywords.");
                throw new Exception("SQL query contains unsafe keywords.");
            }               
            return query;
        }

        #endregion
    }
}