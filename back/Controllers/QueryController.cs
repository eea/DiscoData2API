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
            public QueryController(ILogger<QueryController> logger, MongoService mongoService, DremioService dremioService)
            {
                  _logger = logger; 
                  _mongoService = mongoService;
                  _dremioService = dremioService;
            }

            [HttpGet("GetCatalog")]
            public async Task<ActionResult<List<MongoDocument>>> GetCatalog()
            {
                  return await _mongoService.GetAllAsync();
            }

            [HttpPost("{id}")]
            public async Task<ActionResult<string>> PostQuery(string id, [FromBody] QueryRequest request)
            {
                  _logger.LogInformation($"Received query request for id {id} with fields {request.Fields}, filters {request.Filters}, limit {request.Limit}, and offset {request.Offset}");
                  MongoDocument mongoDoc = await _mongoService.GetById(id);

                  if (mongoDoc == null)
                  {
                        _logger.LogError($"Query with id {id} not found");
                        return NotFound();
                  }

                  string? dremioToken = await _dremioService.GetToken();

                  if(dremioToken == null)
                  {
                        _logger.LogError("Dremio token is null");
                        return StatusCode(500);
                  }
                  else
                  {
                        _logger.LogInformation("Dremio token received");
                         //Execute the query and return the result   
                  }     

                
                  return Ok(mongoDoc.Query);
            }
      }
}