using DiscoData2API.Services;
using DiscoData2API.Model;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/data-catalog")]
    public class DataCatalogController(ILogger<DataCatalogController> logger, DremioService dremioService, MongoService mongoService) : ControllerBase
    {
        private readonly ILogger<DataCatalogController> _logger = logger;
        private readonly DremioService _dremioService = dremioService;
        private readonly MongoService _mongoService = mongoService;
        private readonly int _timeout = dremioService._timeout;

        private const string DremioBasePath = "discoData";
        private const string DremioTier = "gold";
        private const string AnonymousOwner = "Anonymous";

        /// <summary>Create a new view in Dremio and register it in MongoDB</summary>
        /// <param name="request">View name, SQL, and optional owner ID</param>
        /// <response code="201">View created successfully</response>
        /// <response code="400">Invalid request or Dremio error</response>
        /// <response code="404">Owner not found</response>
        [HttpPost("views")]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesDefaultResponseType]
        public async Task<ActionResult> CreateView([FromBody] CreateViewRequest request)
        {
            string ownerName;
            string ownerId;

            var resolvedOwner = await _mongoService.GetOwnerByNameAsync(
                !string.IsNullOrWhiteSpace(request.OwnerName) ? request.OwnerName : AnonymousOwner);

            if (resolvedOwner == null)
                return NotFound($"Owner '{request.OwnerName ?? AnonymousOwner}' not found.");

            ownerName = resolvedOwner.Name;
            ownerId = resolvedOwner.Id;

            var mongoId = MongoDB.Bson.ObjectId.GenerateNewId().ToString();
            var dremioPath = new[] { DremioBasePath, DremioTier, ownerName, mongoId };
            var viewPath = string.Join(".", dremioPath);

            try
            {
                await _dremioService.ApiPost<object>("catalog", new
                {
                    entityType = "dataset",
                    type = "VIRTUAL_DATASET",
                    path = dremioPath,
                    sql = request.Sql,
                    sqlContext = new[] { DremioBasePath, DremioTier, ownerName }
                });

                _logger.LogInformation("Created Dremio view at {Path}", viewPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create Dremio view at {Path}", viewPath);
                return BadRequest($"Dremio error: {ex.Message}");
            }

            var viewDoc = new ViewDocument
            {
                Id = mongoId,
                OwnerId = ownerId,
                Path = viewPath,
                DisplayName = request.DisplayName,
                Description = request.Description,
                IsActive = true,
                Template = null
            };

            await _mongoService.InsertViewAsync(viewDoc);

            var body = new { id = viewDoc.Id, displayName = viewDoc.DisplayName, path = viewDoc.Path, owner = ownerName };
            return Created($"/api/data-products/views/{viewDoc.Id}", body);
        }

        /// <summary>Get a gold view metadata and SQL definition by ID</summary>
        /// <param name="viewId">MongoDB view ID</param>
        /// <response code="200">Returns view details</response>
        /// <response code="404">View not found</response>
        [HttpGet("views/{viewId}")]
        public async Task<ActionResult> GetView(string viewId)
        {
            var view = await _mongoService.GetViewByIdAsync(viewId);
            if (view == null)
                return NotFound($"View '{viewId}' not found.");

            try
            {
                var dataset = await _dremioService.ApiGet<DremioCatalogDataset>($"catalog/by-path/{view.Path.Replace('.', '/')}");
                return Ok(new
                {
                    id = view.Id,
                    displayName = view.DisplayName,
                    description = view.Description,
                    path = view.Path,
                    sql = dataset.Sql
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fetch Dremio view {Path}", view.Path);
                return Ok(new
                {
                    id = view.Id,
                    displayName = view.DisplayName,
                    description = view.Description,
                    path = view.Path,
                    sql = (string?)null
                });
            }
        }

        /// <summary>Delete a view from Dremio and soft-delete it in MongoDB</summary>
        /// <param name="viewId">MongoDB view ID</param>
        /// <response code="204">View deleted</response>
        /// <response code="404">View not found</response>
        [HttpDelete("views/{viewId}")]
        public async Task<ActionResult> DeleteView(string viewId)
        {
            var view = await _mongoService.GetViewByIdAsync(viewId);
            if (view == null)
                return NotFound($"View '{viewId}' not found.");

            try
            {
                var entity = await _dremioService.ApiGet<DremioEntityResponse>($"catalog/by-path/{view.Path.Replace('.', '/')}");
                await _dremioService.ApiDelete($"catalog/{entity.Id}");
                _logger.LogInformation("Deleted Dremio view {Path}", view.Path);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not delete Dremio view {Path}, removing from MongoDB only", view.Path);
            }

            await _mongoService.DeleteViewAsync(viewId);
            return NoContent();
        }

        /// <summary>Update the SQL and/or metadata of an existing gold view</summary>
        /// <param name="viewId">MongoDB view ID</param>
        /// <param name="request">Fields to update</param>
        /// <response code="200">View updated successfully</response>
        /// <response code="400">Dremio error</response>
        /// <response code="404">View not found</response>
        [HttpPut("views/{viewId}")]
        public async Task<ActionResult> UpdateView(string viewId, [FromBody] UpdateViewRequest request)
        {
            var view = await _mongoService.GetViewByIdAsync(viewId);
            if (view == null)
                return NotFound($"View '{viewId}' not found.");

            if (!string.IsNullOrWhiteSpace(request.Sql))
            {
                try
                {
                    var rawJson = await _dremioService.ApiGet<string>($"catalog/by-path/{view.Path.Replace('.', '/')}");
                    var entity = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(rawJson)!;
                    entity["sql"] = JsonSerializer.SerializeToElement(request.Sql);
                    var updatedJson = JsonSerializer.Serialize(entity);
                    var entityId = entity["id"].GetString()!;
                    _logger.LogWarning("Updating Dremio entity id={Id} body={Body}", entityId, updatedJson);
                    await _dremioService.ApiPutRaw($"catalog/{entityId}", updatedJson);
                    _logger.LogInformation("Updated Dremio view {Path}", view.Path);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to update Dremio view {Path}", view.Path);
                    return BadRequest($"Dremio error: {ex.Message}");
                }
            }

            await _mongoService.UpdateViewAsync(viewId, request.DisplayName, request.Description);

            return Ok(new { id = viewId, displayName = request.DisplayName ?? view.DisplayName, description = request.Description ?? view.Description });
        }

        /// <summary>List all folders (schemas) under discoData/silver in the Dremio catalog</summary>
        [HttpGet("schemas")]
        public async Task<ActionResult> GetSchemas()
        {
            try
            {
                var container = await _dremioService.ApiGet<DremioCatalogContainer>("catalog/by-path/discoData/silver");
                var schemas = container.Children
                    .Where(c => c.Type == "CONTAINER")
                    .Select(c => new DremioSchema
                    {
                        Schema = string.Join(".", c.Path),
                        SchemaName = c.Path[^1]
                    })
                    .ToList();

                return Ok(schemas);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetSchemas");
                return StatusCode(500, ex.Message);
            }
        }

        /// <summary>List all views (tables) under a given schema folder in discoData/silver, recursing into sub-folders</summary>
        /// <param name="schema">Folder name under discoData/silver</param>
        [HttpGet("schemas/{schema}/tables")]
        public async Task<ActionResult> GetTables(string schema)
        {
            try
            {
                var container = await _dremioService.ApiGet<DremioCatalogContainer>($"catalog/by-path/discoData/silver/{schema}");
                var tables = await CollectDatasetsAsync(container.Children);
                return Ok(tables.Select(c => new DremioTable { TableName = c.Path[^1] }));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetTables");
                return StatusCode(500, ex.Message);
            }
        }

        /// <summary>List all columns for a view in discoData/silver</summary>
        /// <param name="schema">Folder name under discoData/silver</param>
        /// <param name="table">View name</param>
        [HttpGet("schemas/{schema}/tables/{table}/columns")]
        public async Task<ActionResult> GetColumns(string schema, string table)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
            try
            {
                var container = await _dremioService.ApiGet<DremioCatalogContainer>($"catalog/by-path/discoData/silver/{schema}");
                var datasets = await CollectDatasetsAsync(container.Children);
                var match = datasets.FirstOrDefault(d => d.Path[^1].Equals(table, StringComparison.OrdinalIgnoreCase));

                if (match == null)
                    return NotFound($"Table '{table}' not found under schema '{schema}'.");

                var tableSchema = string.Join(".", match.Path[..^1]);
                var query = $"SELECT * FROM INFORMATION_SCHEMA.\"COLUMNS\" WHERE TABLE_SCHEMA = '{tableSchema}' AND TABLE_NAME = '{table}'";
                var result = await _dremioService.ExecuteQuery(query, cts.Token);
                var columns = JsonSerializer.Deserialize<List<DremioColumn>>(result) ?? [];
                return Ok(columns);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetColumns");
                return StatusCode(500, ex.Message);
            }
        }

        /// <summary>Get the SQL definition of a silver view from the Dremio catalog</summary>
        /// <param name="schema">Folder name under discoData/silver</param>
        /// <param name="table">View name</param>
        [HttpGet("schemas/{schema}/tables/{table}/definition")]
        public async Task<ActionResult> GetViewDefinition(string schema, string table)
        {
            try
            {
                var container = await _dremioService.ApiGet<DremioCatalogContainer>($"catalog/by-path/discoData/silver/{schema}");
                var datasets = await CollectDatasetsAsync(container.Children);
                var match = datasets.FirstOrDefault(d => d.Path[^1].Equals(table, StringComparison.OrdinalIgnoreCase));

                if (match == null)
                    return NotFound($"Table '{table}' not found under schema '{schema}'.");

                var dataset = await _dremioService.ApiGet<DremioCatalogDataset>($"catalog/{match.Id}");
                return Ok(new { name = table, sql = dataset.Sql });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetViewDefinition");
                return StatusCode(500, ex.Message);
            }
        }


        #region helper

        public class CreateViewRequest
        {
            public string? DisplayName { get; set; }

            [Required]
            public string Sql { get; set; } = null!;

            public string? OwnerName { get; set; }

            public string? Description { get; set; }
        }

private async Task<List<DremioCatalogItem>> CollectDatasetsAsync(List<DremioCatalogItem> items)
        {
            var datasets = new List<DremioCatalogItem>();
            foreach (var item in items)
            {
                if (item.Type == "DATASET")
                {
                    datasets.Add(item);
                }
                else if (item.Type == "CONTAINER")
                {
                    var child = await _dremioService.ApiGet<DremioCatalogContainer>($"catalog/{item.Id}");
                    datasets.AddRange(await CollectDatasetsAsync(child.Children));
                }
            }
            return datasets;
        }

        public class UpdateViewRequest
        {
            public string? Sql { get; set; }
            public string? DisplayName { get; set; }
            public string? Description { get; set; }
        }

        private class DremioEntityResponse
        {
            [JsonPropertyName("id")]
            public string Id { get; set; } = null!;
        }

#endregion
    }
}
