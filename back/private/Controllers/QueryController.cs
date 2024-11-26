using DiscoData2API_Priv.Services;
using DiscoData2API_Priv.Class;
using DiscoData2API_Priv.Model;
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
        /// Create a query (MongoDB)
        /// </summary>
        /// <param name="request"></param>
        /// <returns>Return the Json document saved</returns>
        [HttpPost("CreateQuery")]
        public async Task<ActionResult<MongoDocument>> CreateQuery([FromBody] MongoDocument request)
        {
            return await _mongoService.CreateAsync(new MongoDocument()
            {
                Query = request.Query,
                Name = request.Name,
                Version = request.Version,
                Date = DateTime.Now,
                Fields = request.Fields
            });
        }

        /// <summary>
        /// Read a query (MongoDB)
        /// </summary>
        /// <param name="id"></param>
        /// <returns>Return a MongoDocument</returns>
        [HttpGet("ReadQuery/{id}")]
        public async Task<ActionResult<MongoDocument>> ReadQuery(string id)
        {
            return await _mongoService.ReadAsync(id);
        }

        /// <summary>
        /// Update a query (MongoDB)
        /// </summary>
        /// <param name="request"></param>
        /// <returns>Return the updated MongoDocument</returns>
        [HttpPost("UpdateQuery")]
        public async Task<ActionResult<MongoDocument>> UpdateQuery([FromBody] MongoDocument request)
        {
            return await _mongoService.UpdateAsync(request);
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
        public async Task<ActionResult<List<MongoDocument>>> GetMongoCatalog()
        {
            return await _mongoService.GetAllAsync();
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
                MongoDocument mongoDoc = await _mongoService.ReadAsync(id);

                if (mongoDoc == null)
                {
                    _logger.LogError($"Query with id {id} not found");
                    return NotFound();
                }
                else
                {
                    mongoDoc.Query = UpdateQueryString(mongoDoc.Query, request.Fields, request.Limit);
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

        /// <summary>
        /// Update the query string with fields and limit parameters
        /// </summary>
        /// <param name="query"></param>
        /// <param name="fields"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        private string UpdateQueryString(string query, string[]? fields, int? limit)
        {
            //Update fields returned by query
            fields = fields != null && fields.Length > 0 ? fields : new string[] { "*" };
            query = query.Replace("*", string.Join(",", fields));

            //Update limit of query
            limit = limit.HasValue && limit != 0 ? limit.Value : _defaultLimit;
            if (query.Contains("LIMIT"))
            {
                query = System.Text.RegularExpressions.Regex.Replace(query, @"LIMIT\s+\d+", $"LIMIT {limit}");
            }
            else
            {
                query += $" LIMIT {limit}";
            }

            return query;
        }

        #endregion
    }
}