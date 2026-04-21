using DiscoData2API.Services;
using DiscoData2API.Class;
using DiscoData2API.Model;
using DiscoData2API.Misc;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using System.Text.Json.Serialization;
using MongoDB.Bson;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Text.RegularExpressions;

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

            return Ok(owners.Select(o => new { id = o.Id, name = o.Name }));
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
        [HttpGet("views/{viewId}")]
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

        private const string DremioBasePath = "discoData";
        private const string DremioTier = "gold";
        private const string AnonymousOwner = "Anonymous";

        /// <summary>Create a new view in Dremio and register it in MongoDB</summary>
        /// <param name="request">View name, SQL, and optional owner ID</param>
        /// <response code="201">View created successfully</response>
        /// <response code="400">Invalid request or Dremio error</response>
        /// <response code="404">Owner not found</response>
        [HttpPost("views")]
        public async Task<ActionResult> CreateView([FromBody] CreateViewRequest request)
        {
            string ownerName = AnonymousOwner;
            string? ownerId = null;

            if (!string.IsNullOrWhiteSpace(request.OwnerId))
            {
                var (owner, _) = await _mongoService.GetViewsByOwnerIdAsync(request.OwnerId);
                if (owner == null)
                    return NotFound($"Owner '{request.OwnerId}' not found.");
                ownerName = owner.Name;
                ownerId = owner.Id;
            }

            var dremioPath = new[] { DremioBasePath, DremioTier, ownerName, request.ViewName };
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
                OwnerId = ownerId ?? string.Empty,
                Path = viewPath,
                Description = request.Description,
                IsActive = true,
                Template = null
            };

            await _mongoService.InsertViewAsync(viewDoc);

            return CreatedAtAction(null, null, new
            {
                id = viewDoc.Id,
                name = viewDoc.Name,
                path = viewDoc.Path,
                owner = ownerName
            });
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

        #region helper

        private static readonly HashSet<string> _allowedAggregateFunctions = new(StringComparer.OrdinalIgnoreCase)
            { "COUNT", "SUM", "AVG", "MIN", "MAX", "DATE_TRUNC" };
        private static readonly HashSet<string> _allowedGranularities = new(StringComparer.OrdinalIgnoreCase)
            { "day", "week", "month", "quarter", "year" };

        private string BuildDirectQuery(string dataset, string[]? fields, int? limit, int? offset, FilterDefinition[]? filters, string[]? groupBy = null, AggregateDefinition[]? aggregates = null)
        {
            var quotedTable = string.Join(".", dataset.Split('.').Select(p => $"\"{p}\""));
            var selectedFields = fields != null && fields.Length > 0
                ? string.Join(", ", fields.Select(f => $"\"{f}\""))
                : "*";
            limit = limit.HasValue && limit != 0 ? limit.Value : _defaultLimit;

            // Build aggregate expressions
            if (aggregates != null && aggregates.Length > 0)
            {
                var aggExpressions = aggregates.Select(a =>
                {
                    if (!_allowedAggregateFunctions.Contains(a.Function))
                        throw new SQLFormattingException($"Aggregate function '{a.Function}' is not allowed.");

                    var func = a.Function.ToUpperInvariant();
                    var field = a.Field == "*" ? "*" : $"\"{a.Field}\"";
                    string expr;

                    if (func == "DATE_TRUNC")
                    {
                        if (string.IsNullOrWhiteSpace(a.Granularity) || !_allowedGranularities.Contains(a.Granularity))
                            throw new SQLFormattingException($"DATE_TRUNC requires a valid granularity (day, week, month, quarter, year).");
                        expr = $"DATE_TRUNC('{a.Granularity.ToLowerInvariant()}', {field})";
                    }
                    else
                    {
                        expr = $"{func}({field})";
                    }

                    return string.IsNullOrWhiteSpace(a.Alias) ? expr : $"{expr} AS \"{a.Alias}\"";
                });

                var fieldsPart = selectedFields == "*" ? "" : selectedFields + ", ";
                selectedFields = fieldsPart + string.Join(", ", aggExpressions);
            }

            var query = $"SELECT {selectedFields} FROM {quotedTable}";

            if (filters != null && filters.Length > 0)
            {
                var filterClause = string.Join(" ", filters.Select(BuildFilterClause));
                query += $" WHERE 1=1 {filterClause}";
            }

            if (groupBy != null && groupBy.Length > 0)
                query += $" GROUP BY {string.Join(", ", groupBy.Select(g => $"\"{g}\""))}";

            query += $" LIMIT {limit}";

            if (offset.HasValue && offset > 0)
                query += $" OFFSET {offset}";

            if (!SQLExtensions.ValidateSQL(query))
            {
                _logger.LogWarning("SQL query contains unsafe keywords.");
                throw new SQLFormattingException("SQL query contains unsafe keywords.");
            }

            return query;
        }

        private static string BuildFilterClause(FilterDefinition filter)
        {
            var values = filter.Values?.Select(QuoteValue).ToArray() ?? [];

            return filter.Condition.ToUpper().Trim() switch
            {
                "BETWEEN" => $" {filter.Concat} (\"{filter.FieldName}\" BETWEEN {string.Join(" AND ", values)})",
                "IN"      => $" {filter.Concat} (\"{filter.FieldName}\" IN ({string.Join(", ", values)}))",
                _         => $" {filter.Concat} (\"{filter.FieldName}\" {filter.Condition} {string.Join(", ", values)})"
            };
        }

        private static string QuoteValue(string value)
        {
            if (value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
                return "NULL";
            if (value.StartsWith('\'') && value.EndsWith('\'') && value.Length >= 2)
                return value;
            if (double.TryParse(value, System.Globalization.NumberStyles.Any,
                    System.Globalization.CultureInfo.InvariantCulture, out _))
                return value;
            return $"'{value.Replace("'", "''")}'";
        }

        public class CreateViewRequest
        {
            [Required]
            public string ViewName { get; set; } = null!;

            [Required]
            public string Sql { get; set; } = null!;

            public string? OwnerId { get; set; }

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
