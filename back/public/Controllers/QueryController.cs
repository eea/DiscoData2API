using DiscoData2API.Class;
using DiscoData2API.Models;
using DiscoData2API.Services;
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

        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoDocument>>> GetMongoCatalog()
        {
            return await _mongoService.GetAllAsync();
        }

        [HttpPost("{id}")]
        public async Task<ActionResult<string>> ExecuteQuery(string id, [FromBody] QueryRequest request)
        {
            id = "672b84ef75e2d0b792658f24";   //for debugging purposes
            MongoDocument mongoDoc = await _mongoService.GetById(id);

            if (mongoDoc == null)
            {
                _logger.LogError($"Query with id {id} not found");
                return NotFound();
            }

            mongoDoc.Query = UpdateQueryString(mongoDoc.Query, request.Fields, request.Limit);

            var result = await _dremioService.ExecuteQuery(mongoDoc.Query);

            return result;
        }

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
    }
}