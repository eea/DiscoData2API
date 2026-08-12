using DiscoData2API.Services;
using DiscoData2API.Model;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;

namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/data-catalog")]
    public class DataCatalogController(ILogger<DataCatalogController> logger, MongoService mongoService) : ControllerBase
    {
        private readonly ILogger<DataCatalogController> _logger = logger;
        private readonly MongoService _mongoService = mongoService;

        /// <summary>Create a new owner, or return the existing one if the name is already taken</summary>
        /// <param name="request">Owner name</param>
        /// <response code="200">Owner already existed, returned as-is</response>
        /// <response code="201">Owner created</response>
        [HttpPost("owners")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status201Created)]
        public async Task<ActionResult> CreateOwner([FromBody] CreateOwnerRequest request)
        {
            var existing = await _mongoService.GetOwnerByNameAsync(request.Name);
            if (existing != null)
                return Ok(new { id = existing.Id, name = existing.Name, displayName = existing.DisplayName });

            var now = DateTime.UtcNow;
            var owner = new OwnerDocument { Name = request.Name, DisplayName = request.DisplayName, IsActive = true, CreatedAt = now, UpdatedAt = now };
            await _mongoService.InsertOwnerAsync(owner);

            return Created($"/api/data-catalog/owners/{owner.Id}", new { id = owner.Id, name = owner.Name, displayName = owner.DisplayName });
        }

        #region DTOs

        public class CreateOwnerRequest
        {
            [Required]
            public string Name { get; set; } = null!;

            public string? DisplayName { get; set; }
        }

        #endregion

        // ────────────────────────────────────────────────────────────────────────
        // Everything below is commented out: replaced by SavedQueryController,
        // PublishedDatasetController, and the DuckDB sidecar (services/query-engine).
        // ────────────────────────────────────────────────────────────────────────

        // /// <summary>Create a new view in Dremio and register it in MongoDB</summary>
        // [HttpPost("views")]
        // public async Task<ActionResult> CreateView([FromBody] CreateViewRequest request) { ... }

        // /// <summary>Get a gold view metadata and SQL definition by ID</summary>
        // [HttpGet("views/{viewId}")]
        // public async Task<ActionResult> GetView(string viewId) { ... }

        // /// <summary>Update the SQL and/or metadata of an existing gold view</summary>
        // [HttpPut("views/{viewId}")]
        // public async Task<ActionResult> UpdateView(string viewId, [FromBody] UpdateViewRequest request) { ... }

        // /// <summary>List all folders (schemas) under discoData/silver in the Dremio catalog</summary>
        // [HttpGet("schemas")]
        // public async Task<ActionResult> GetSchemas() { ... }

        // /// <summary>List all views (tables) under a given schema folder in discoData/silver</summary>
        // [HttpGet("schemas/{schema}/tables")]
        // public async Task<ActionResult> GetTables(string schema) { ... }

        // /// <summary>List all columns for a view in discoData/silver</summary>
        // [HttpGet("schemas/{schema}/tables/{table}/columns")]
        // public async Task<ActionResult> GetColumns(string schema, string table) { ... }

        // /// <summary>Get the SQL definition of a silver view from the Dremio catalog</summary>
        // [HttpGet("schemas/{schema}/tables/{table}/definition")]
        // public async Task<ActionResult> GetViewDefinition(string schema, string table) { ... }
    }
}
