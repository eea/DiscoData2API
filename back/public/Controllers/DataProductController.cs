// ────────────────────────────────────────────────────────────────────────
// Entire controller commented out: query execution moved to the DuckDB
// sidecar (services/query-engine). Saved queries served by
// SavedQueryController; catalog by PublishedDatasetController.
// ────────────────────────────────────────────────────────────────────────

// using DiscoData2API.Services;
// using DiscoData2API.Class;
// using DiscoData2API.Misc;
// using Microsoft.AspNetCore.Mvc;
// using System.Text.Json;
// using System.Text.Json.Serialization;
// using MongoDB.Bson;
//
// namespace DiscoData2API.Controllers
// {
//     [ApiController]
//     [Route("api/data-products")]
//     public class DataProductController(ILogger<DataProductController> logger, DremioService dremioService, MongoService mongoService) : ControllerBase
//     {
//         private readonly ILogger<DataProductController> _logger = logger;
//         private readonly DremioService _dremioService = dremioService;
//         private readonly MongoService _mongoService = mongoService;
//         private readonly int _defaultLimit = dremioService._limit;
//         private readonly int _timeout = dremioService._timeout;
//
//         private static readonly string[] _filterableTypes = ["CHAR", "VARCHAR", "DATE", "TIMESTAMP", "TIME"];
//
//         // GET  views/{viewId}/schema  — Dremio INFORMATION_SCHEMA query
//         // GET  views/{viewId}/data    — Dremio query execution
//         // POST views/{viewId}/data    — Dremio query with filters/aggregates
//         // All replaced by the DuckDB sidecar.
//     }
// }
