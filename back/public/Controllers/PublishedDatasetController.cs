using DiscoData2API.Misc;
using DiscoData2API.Services;
using DiscoData2API.Model;
using Microsoft.AspNetCore.Mvc;

namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/data-catalog/published")]
    public class PublishedDatasetController(ILogger<PublishedDatasetController> logger, MongoService mongoService) : ControllerBase
    {
        private readonly ILogger<PublishedDatasetController> _logger = logger;
        private readonly MongoService _mongoService = mongoService;

        /// <summary>List all published datasets (latest versions only), grouped by dataset</summary>
        /// <response code="200">Returns published datasets grouped by dataset name</response>
        [HttpGet]
        public async Task<ActionResult> List()
        {
            var all = await _mongoService.GetPublishedDatasetsAsync();

            var grouped = all
                .Where(d => d.Version == "latest")
                .GroupBy(d => d.Dataset)
                .Select(g => new
                {
                    dataset = g.Key,
                    tables = g.Select(d => new
                    {
                        table = d.Table,
                        description = d.Description,
                        rowCount = d.RowCount,
                        sizeBytes = d.SizeBytes,
                        promotedAt = d.PromotedAt
                    })
                });

            return Ok(grouped);
        }

        /// <summary>List all tables for a given dataset (schema)</summary>
        /// <param name="dataset">Dataset name (e.g. CO2_emissions)</param>
        /// <response code="200">Returns the list of tables</response>
        [HttpGet("{dataset}")]
        public async Task<ActionResult> GetTables(string dataset)
        {
            var tables = await _mongoService.GetPublishedTablesByDatasetAsync(dataset);

            return Ok(tables.Select(d => new
            {
                table = d.Table,
                description = d.Description,
                columns = d.Columns.Select(c => new { name = c.Name, type = c.Type, description = c.Description }),
                partitionKeys = d.PartitionKeys,
                sortKeys = d.SortKeys,
                rowCount = d.RowCount,
                sizeBytes = d.SizeBytes,
                promotedAt = d.PromotedAt
            }));
        }

        /// <summary>Get a published dataset manifest including file URLs</summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="table">Table name</param>
        /// <param name="version">Version (defaults to "latest")</param>
        /// <response code="200">Returns the full manifest with file URLs</response>
        /// <response code="404">Dataset not found</response>
        [HttpGet("{dataset}/{table}")]
        public async Task<ActionResult> GetManifest(string dataset, string table, [FromQuery] string version = "latest")
        {
            var doc = await _mongoService.GetPublishedDatasetAsync(dataset, table, version);
            if (doc == null)
                return NotFound($"Published dataset '{dataset}.{table}@{version}' not found.");

            return Ok(new
            {
                dataset = doc.Dataset,
                table = doc.Table,
                version = doc.Version,
                description = doc.Description,
                licence = doc.Licence,
                files = doc.Files,
                columns = doc.Columns.Select(c => new { name = c.Name, type = c.Type, description = c.Description }),
                partitionKeys = doc.PartitionKeys,
                sortKeys = doc.SortKeys,
                rowCount = doc.RowCount,
                sizeBytes = doc.SizeBytes,
                promotedAt = doc.PromotedAt,
                promotedBy = doc.PromotedBy
            });
        }

        /// <summary>Get columns for a specific published table (matches the silver catalog shape)</summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="table">Table name</param>
        /// <response code="200">Returns columns</response>
        /// <response code="404">Dataset not found</response>
        [HttpGet("{dataset}/{table}/columns")]
        public async Task<ActionResult> GetColumns(string dataset, string table)
        {
            var doc = await _mongoService.GetPublishedDatasetAsync(dataset, table);
            if (doc == null)
                return NotFound($"Published dataset '{dataset}.{table}' not found.");

            return Ok(doc.Columns.Select(c => new
            {
                COLUMN_NAME = c.Name,
                DATA_TYPE = c.Type,
                IS_NULLABLE = "YES",
                description = c.Description
            }));
        }

        /// <summary>Upsert a published dataset entry. Used by the S3 catalog scanner.</summary>
        /// <response code="200">Entry upserted</response>
        [HttpPut("{dataset}/{table}")]
        [RequireWriteKey]
        public async Task<ActionResult> Upsert(string dataset, string table, [FromBody] UpsertPublishedDatasetRequest body)
        {
            var doc = new PublishedDatasetDocument
            {
                Dataset = dataset,
                Table = table,
                Version = body.Version ?? "latest",
                Files = body.Files ?? [],
                Columns = (body.Columns ?? []).Select(c => new PublishedColumn
                {
                    Name = c.Name,
                    Type = c.Type,
                    Description = c.Description
                }).ToList(),
                RowCount = body.RowCount,
                SizeBytes = body.SizeBytes,
                PromotedAt = DateTime.UtcNow,
                PromotedBy = body.PromotedBy ?? "scanner",
                IsActive = true
            };

            await _mongoService.UpsertPublishedDatasetAsync(doc);
            return Ok(new { dataset, table, version = doc.Version, status = "upserted" });
        }

        /// <summary>Resolve a logical reference to file URLs. Used by the query engine to build read_parquet() calls.</summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="table">Table name</param>
        /// <param name="version">Version (defaults to "latest")</param>
        /// <response code="200">Returns the file URLs</response>
        /// <response code="404">Dataset not found</response>
        [HttpGet("{dataset}/{table}/files")]
        public async Task<ActionResult> ResolveFiles(string dataset, string table, [FromQuery] string version = "latest")
        {
            var doc = await _mongoService.GetPublishedDatasetAsync(dataset, table, version);
            if (doc == null)
                return NotFound($"Published dataset '{dataset}.{table}@{version}' not found.");

            return Ok(new
            {
                dataset = doc.Dataset,
                table = doc.Table,
                version = doc.Version,
                files = doc.Files
            });
        }
    }

    public class UpsertPublishedDatasetRequest
    {
        public string? Version { get; set; }
        public List<string>? Files { get; set; }
        public List<UpsertColumnRequest>? Columns { get; set; }
        public long RowCount { get; set; }
        public long SizeBytes { get; set; }
        public string? PromotedBy { get; set; }
    }

    public class UpsertColumnRequest
    {
        public string Name { get; set; } = null!;
        public string Type { get; set; } = null!;
        public string? Description { get; set; }
    }
}
