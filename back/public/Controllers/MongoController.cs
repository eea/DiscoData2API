using DiscoData2API.Models;
using DiscoData2API.Services;
using Microsoft.AspNetCore.Mvc;

namespace DiscoData2API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MongoController : ControllerBase
    {
        private readonly MongoService _mongoService;

        public MongoController(MongoService mongoService)
        {
            _mongoService = mongoService;
        }

        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoDocument>>> Get()
        {
            return await _mongoService.GetAllAsync();
        }

        [HttpGet("GetDocument/{id}")]
        public async Task<ActionResult<MongoDocument>> Get(string id)
        {
            return await _mongoService.GetById(id);
        }
    }
}