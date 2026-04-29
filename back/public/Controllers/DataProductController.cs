using DiscoData2API.Services;
using DiscoData2API.Class;
using DiscoData2API.Misc;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using System.Text.Json.Serialization;
using MongoDB.Bson;

namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/data-products")]
    public class DataProductController(ILogger<DataProductController> logger, DremioService dremioService, MongoService mongoService) : ControllerBase
    {
        private readonly ILogger<DataProductController> _logger = logger;
        private readonly DremioService _dremioService = dremioService;
        private readonly MongoService _mongoService = mongoService;
        private readonly int _defaultLimit = dremioService._limit;
        private readonly int _timeout = dremioService._timeout;

        private static readonly string[] _filterableTypes = ["CHAR", "VARCHAR", "DATE", "TIMESTAMP", "TIME"];

       
        /// <summary>List all available data product owners (MONGO)</summary>
        /// <response code="200">Returns list of owners</response>
        /// <response code="404">If no owners found</response>
        [HttpGet("owners")]
        public async Task<ActionResult> GetOwners()
        {
            var owners = await _mongoService.GetAllOwnersAsync();
            if (owners.Count == 0)
                return NotFound("No data products found.");

            return Ok(owners.Select(o => new { id = o.Id, name = o.Name, displayName = o.DisplayName }));
        }

        /// <summary>Get all views for a given owner with their query templates (MONGO)</summary>
        /// <param name="ownerId">MongoDB owner ID</param>
        /// <response code="200">Returns views and templates</response>
        /// <response code="404">If the owner is not found</response>
        [HttpGet("owners/{ownerId}/views")]
        public async Task<ActionResult> GetViewsByOwner(string ownerId)
        {
            var (owner, views) = await _mongoService.GetViewsByOwnerIdAsync(ownerId);

            if (owner == null)
                return NotFound($"Owner '{ownerId}' not found.");

            return Ok(new
            {
                id = owner.Id,
                owner = owner.Name,
                views = views.Select(v => new
                {
                    id = v.Id,
                    name = v.Name,
                    description = v.Description,
                    queryUrl = Url.Action(nameof(GetView), new { viewId = v.Id }),
                    template = v.Template == null ? (JsonElement?)null : JsonSerializer.Deserialize<JsonElement>(v.Template.ToJson())
                })
            });
        }

        /// <summary>Get schema metadata for a view :fields, filterable columns, max limit (DREMIO)</summary>
        /// <param name="viewId">MongoDB view ID</param>
        /// <response code="200">Returns view schema</response>
        /// <response code="404">If the view is not found</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("views/{viewId}/schema")]
        public async Task<ActionResult> GetView(string viewId)
        {
            var view = await _mongoService.GetViewByIdAsync(viewId);
            if (view == null)
                return NotFound($"View '{viewId}' not found.");

            var viewName = view.Path;
            var parts = viewName.Split('.');
            var tableName = parts[^1];
            var tableSchema = string.Join(".", parts[..^1]);

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
            try
            {
                var query = $"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.\"COLUMNS\" WHERE TABLE_SCHEMA = '{tableSchema}' AND TABLE_NAME = '{tableName}'";
                var json = await _dremioService.ExecuteQuery(query, cts.Token);
                var columns = JsonSerializer.Deserialize<List<Dictionary<string, string>>>(json);

                if (columns == null || columns.Count == 0)
                    return NotFound($"View schema for '{view.Name}' not found in Dremio.");

                var fields = columns.Select(c => c["COLUMN_NAME"]).ToList();
                var filters = columns
                    .Where(c => _filterableTypes.Any(t => c["DATA_TYPE"].Contains(t, StringComparison.OrdinalIgnoreCase)))
                    .Select(c => c["COLUMN_NAME"])
                    .ToList();

                return Ok(new { id = view.Id, name = view.Name, description = view.Description, fields, filters, maxLimit = _defaultLimit });
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (Exception ex)
            {
                _logger.LogError(message: ex.Message);
                var errmsg = ex is Grpc.Core.RpcException rpcEx
                    ? rpcEx.Status.Detail.Split(['\r', '\n'])[0]
                    : ex.Message;
                return BadRequest(errmsg);
            }
        }

        /// <summary>Execute a view with no filters and an optional limit override (DREMIO)</summary>
        /// <param name="viewId">MongoDB view ID</param>
        /// <param name="limit">Max rows to return (overrides default)</param>
        /// <response code="200">Returns view data</response>
        /// <response code="404">If the view is not found</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("views/{viewId}/data")]
        public async Task<ActionResult> GetViewData(string viewId, [FromQuery] int? limit = null)
        {
            var view = await _mongoService.GetViewByIdAsync(viewId);
            if (view == null)
                return NotFound($"View '{viewId}' not found.");

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
            try
            {
                var finalQuery = BuildDirectQuery(view.Path, null, limit, null, null);
                _logger.LogInformation("Executing view {ViewId}: {Query}", viewId, finalQuery);
                var result = await _dremioService.ExecuteQuery(finalQuery, cts.Token);
                return Ok(JsonSerializer.Deserialize<JsonElement>(result));
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (Exception ex)
            {
                _logger.LogError(message: ex.Message);
                var errmsg = ex is Grpc.Core.RpcException rpcEx
                    ? rpcEx.Status.Detail.Split(['\r', '\n'])[0]
                    : ex.Message;
                return BadRequest(errmsg);
            }
        }
        /// <summary>Execute a view with optional field selection, filters, aggregates, grouping, and pagination (DREMIO)</summary>
        /// <param name="viewId">MongoDB view ID</param>
        /// <param name="request">Fields, filters, aggregates, groupBy, limit, offset</param>
        /// <response code="200">Returns query results</response>
        /// <response code="400">If the query fails or contains unsafe SQL</response>
        /// <response code="404">If the view is not found</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("views/{viewId}/data")]
        public async Task<ActionResult> Query(string viewId, [FromBody] QueryRequest request)
        {
            var view = await _mongoService.GetViewByIdAsync(viewId);
            if (view == null)
                return NotFound($"View '{viewId}' not found.");

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
            try
            {
                var finalQuery = BuildDirectQuery(view.Path, request.Fields, request.Limit, request.Offset, request.Filters, request.GroupBy, request.Aggregates);
                _logger.LogInformation("Executing query on view {ViewId}: {Query}", viewId, finalQuery);
                var result = await _dremioService.ExecuteQuery(finalQuery, cts.Token);
                return Ok(JsonSerializer.Deserialize<JsonElement>(result));
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (SQLFormattingException ex)
            {
                _logger.LogError(ex.Message);
                return BadRequest(ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(message: ex.Message);
                var errmsg = ex is Grpc.Core.RpcException rpcEx
                    ? rpcEx.Status.Detail.Split(['\r', '\n'])[0]
                    : ex.Message;
                return BadRequest(errmsg);
            }
        }

        #region helper

        private string BuildDirectQuery(string dataset, string[]? fields, int? limit, int? offset, FilterDefinition[]? filters, string[]? groupBy = null, AggregateDefinition[]? aggregates = null)
            => SqlHelper.BuildQuery(dataset, _defaultLimit, fields, limit, offset, filters, groupBy, aggregates);

        #endregion
    }
}
