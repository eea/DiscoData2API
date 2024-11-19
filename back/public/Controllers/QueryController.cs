using DiscoData2API.Models;
using DiscoData2API.Services;
using DiscoData2API_Library;
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
        private int defaultLimit = 150000;

        public QueryController(ILogger<QueryController> logger, MongoService mongoService, DremioService dremioService)
        {
            _logger = logger;
            _mongoService = mongoService;
            _dremioService = dremioService;
        }

        /// <summary>
        /// Get catalog of queries from MongoDB
        /// </summary>
        /// <returns>Return a list of MongoDocument class</returns>
        /// 
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
            MongoDocument mongoDoc = await _mongoService.GetById(id);

            if (mongoDoc == null)
            {
                _logger.LogError($"Query with id {id} not found");
                return NotFound();
            }
            else
            {  
                mongoDoc.Query = UpdateQueryString(mongoDoc.Query, request.Fields, request.Limit);
            }

            var result = await _dremioService.ExecuteQuery(mongoDoc.Query);

            return result;
        }

        #region helper

        private string UpdateQueryString(string query, string[]? fields, int? limit)
        {
            //Update fields returned by query
            fields = fields != null ? fields : new string[] { "*" };
            query = query.Replace("*", string.Join(",", fields));

            //Update limit of query
            limit = limit.HasValue && limit != 0 ? limit.Value : defaultLimit;
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