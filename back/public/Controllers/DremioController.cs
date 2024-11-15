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
            var login = await _dremioService.ApiLogin();
            if (login != null)
            {
                return login?.Token ?? "Token is null";
            }
            return "Token is null";
        }
    }
}