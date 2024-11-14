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
            public async Task<ActionResult<string>> GetDremioQuery(string id, [FromBody] QueryRequest request)
            {
                  id = "672b84ef75e2d0b792658f24";   //for debugging purposes
                  _logger.LogInformation($"Received query request for id {id} with fields {request.Fields}, filters {request.Filters}, limit {request.Limit}, and offset {request.Offset}");
                  MongoDocument mongoDoc = await _mongoService.GetById(id);

                  if (mongoDoc == null)
                  {
                        _logger.LogError($"Query with id {id} not found");
                        return NotFound();
                  }
                 
                  _logger.LogInformation("Dremio token received");
                  string source = "\"Local S3\".\"datahub-pre-01\".discodata.CO2_emissions.latest.co2cars";
                  var toto = await _dremioService.ExecuteQuery(source, 100);
                  
                  return toto;
            }
      }
}