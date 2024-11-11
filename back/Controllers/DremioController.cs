using DiscoData2API.Services;
using Microsoft.AspNetCore.Mvc;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DiscoData2API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DremioController : ControllerBase
    {
         private readonly DremioService _dremioService;

        public DremioController(DremioService dremioService)
        {
            _dremioService = dremioService;
        }

        [HttpGet("GetToken")]
        public async Task<string> GetToken()
        {
            var token = await _dremioService.GetToken();
            if (token == null)
            {
                return "Token is null";
            }
            return token;
        }
    }
}