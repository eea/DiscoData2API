using DiscoData2API.Services;
using DiscoData2API.Model;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;

namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/data-catalog/saved-queries")]
    public class SavedQueryController(ILogger<SavedQueryController> logger, MongoService mongoService) : ControllerBase
    {
        private readonly ILogger<SavedQueryController> _logger = logger;
        private readonly MongoService _mongoService = mongoService;

        private const string AnonymousOwner = "Anonymous";

        /// <summary>Create a new saved query. If no owner is specified, defaults to Anonymous.</summary>
        /// <param name="request">SQL, display name, optional owner name</param>
        /// <response code="201">Saved query created</response>
        /// <response code="404">Owner not found</response>
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesDefaultResponseType]
        public async Task<ActionResult> Create([FromBody] CreateSavedQueryRequest request)
        {
            var ownerName = !string.IsNullOrWhiteSpace(request.OwnerName) ? request.OwnerName : AnonymousOwner;
            var owner = await _mongoService.GetOwnerByNameAsync(ownerName);
            if (owner == null)
                return NotFound($"Owner '{ownerName}' not found.");

            var now = DateTime.UtcNow;
            var doc = new SavedQueryDocument
            {
                OwnerId = owner.Id,
                DisplayName = request.DisplayName,
                Description = request.Description,
                Sql = request.Sql,
                LogicalRefs = request.LogicalRefs,
                IsActive = true,
                CreatedAt = now,
                UpdatedAt = now
            };

            await _mongoService.InsertSavedQueryAsync(doc);

            _logger.LogInformation("Created saved query {Id} for owner {Owner}", doc.Id, ownerName);

            return Created($"/api/data-catalog/saved-queries/{doc.Id}", new
            {
                id = doc.Id,
                displayName = doc.DisplayName,
                description = doc.Description,
                owner = ownerName
            });
        }

        /// <summary>Get a saved query by ID</summary>
        /// <param name="id">Saved query ID</param>
        /// <response code="200">Returns the saved query</response>
        /// <response code="404">Saved query not found</response>
        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(string id)
        {
            var doc = await _mongoService.GetSavedQueryByIdAsync(id);
            if (doc == null)
                return NotFound($"Saved query '{id}' not found.");

            var owner = await _mongoService.GetOwnerByIdAsync(doc.OwnerId);

            return Ok(new
            {
                id = doc.Id,
                ownerId = doc.OwnerId,
                owner = owner?.Name,
                displayName = doc.DisplayName,
                description = doc.Description,
                sql = doc.Sql,
                logicalRefs = doc.LogicalRefs,
                createdAt = doc.CreatedAt,
                updatedAt = doc.UpdatedAt
            });
        }

        /// <summary>List all saved queries for an owner</summary>
        /// <param name="ownerId">Owner ID</param>
        /// <response code="200">Returns the list of saved queries</response>
        /// <response code="404">Owner not found</response>
        [HttpGet("by-owner/{ownerId}")]
        public async Task<ActionResult> GetByOwner(string ownerId)
        {
            var owner = await _mongoService.GetOwnerByIdAsync(ownerId);
            if (owner == null)
                return NotFound($"Owner '{ownerId}' not found.");

            var queries = await _mongoService.GetSavedQueriesByOwnerAsync(ownerId);

            return Ok(new
            {
                id = owner.Id,
                owner = owner.Name,
                savedQueries = queries.Select(q => new
                {
                    id = q.Id,
                    displayName = q.DisplayName,
                    description = q.Description,
                    createdAt = q.CreatedAt,
                    updatedAt = q.UpdatedAt
                })
            });
        }

        /// <summary>Update a saved query's SQL and/or metadata. Setting ownerName claims an Anonymous query for that owner.</summary>
        /// <param name="id">Saved query ID</param>
        /// <param name="request">Fields to update</param>
        /// <response code="200">Saved query updated</response>
        /// <response code="404">Saved query or new owner not found</response>
        /// <response code="409">Query already belongs to a named owner and cannot be claimed</response>
        [HttpPut("{id}")]
        public async Task<ActionResult> Update(string id, [FromBody] UpdateSavedQueryRequest request)
        {
            var existing = await _mongoService.GetSavedQueryByIdAsync(id);
            if (existing == null)
                return NotFound($"Saved query '{id}' not found.");

            var currentOwner = await _mongoService.GetOwnerByIdAsync(existing.OwnerId);
            var ownerName = currentOwner?.Name;
            string? newOwnerId = null;

            if (!string.IsNullOrWhiteSpace(request.OwnerName) && request.OwnerName != ownerName)
            {
                // Only anonymous queries can change hands; a named owner's query is never moved.
                if (ownerName != AnonymousOwner)
                    return Conflict($"Saved query '{id}' belongs to '{ownerName}' and cannot be claimed.");

                var newOwner = await _mongoService.GetOwnerByNameAsync(request.OwnerName);
                if (newOwner == null)
                    return NotFound($"Owner '{request.OwnerName}' not found.");

                newOwnerId = newOwner.Id;
                ownerName = newOwner.Name;
                _logger.LogInformation("Saved query {Id} claimed by owner {Owner}", id, ownerName);
            }

            await _mongoService.UpdateSavedQueryAsync(id, request.DisplayName, request.Description, request.Sql, request.LogicalRefs, newOwnerId);

            return Ok(new
            {
                id,
                ownerId = newOwnerId ?? existing.OwnerId,
                owner = ownerName,
                displayName = request.DisplayName ?? existing.DisplayName,
                description = request.Description ?? existing.Description
            });
        }

        /// <summary>Soft-delete a saved query</summary>
        /// <param name="id">Saved query ID</param>
        /// <response code="204">Saved query deleted</response>
        /// <response code="404">Saved query not found</response>
        [HttpDelete("{id}")]
        public async Task<ActionResult> Delete(string id)
        {
            var deleted = await _mongoService.DeleteSavedQueryAsync(id);
            if (!deleted)
                return NotFound($"Saved query '{id}' not found.");

            _logger.LogInformation("Soft-deleted saved query {Id}", id);
            return NoContent();
        }

        #region DTOs

        public class CreateSavedQueryRequest
        {
            [Required]
            public string Sql { get; set; } = null!;

            public string? DisplayName { get; set; }
            public string? Description { get; set; }
            public string? OwnerName { get; set; }
            public List<LogicalRef>? LogicalRefs { get; set; }
        }

        public class UpdateSavedQueryRequest
        {
            public string? Sql { get; set; }
            public string? DisplayName { get; set; }
            public string? Description { get; set; }
            public List<LogicalRef>? LogicalRefs { get; set; }

            /// <summary>Claim an Anonymous query for this owner. Ignored if unchanged.</summary>
            public string? OwnerName { get; set; }
        }

        #endregion
    }
}
