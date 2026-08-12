using DiscoData2API.Class;
using DiscoData2API.Misc;
using DiscoData2API.Model;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Collections.Generic;


namespace DiscoData2API.Services
{
    public class MongoService
    {
        private readonly ILogger<MongoService> _logger;
        private readonly IMongoCollection<MongoDocument> _collection;
        private readonly IMongoCollection<OwnerDocument> _owners;
        private readonly IMongoCollection<ViewDocument> _views;
        private readonly IMongoCollection<SavedQueryDocument> _savedQueries;
        private readonly IMongoCollection<PublishedDatasetDocument> _publishedDatasets;

        public MongoService(IOptions<ConnectionSettingsMongo> mongoSettings, ILogger<MongoService> logger)
        {
            _logger = logger;
            var mongoClient = new MongoClient(mongoSettings.Value.ConnectionString);
            var database = mongoClient.GetDatabase(mongoSettings.Value.DatabaseName);
            var discoDataDatabase = mongoClient.GetDatabase(mongoSettings.Value.DiscoDataDatabaseName);
            _collection = database.GetCollection<MongoDocument>(mongoSettings.Value.CollectionName);
            _owners = discoDataDatabase.GetCollection<OwnerDocument>("owners");
            _views = discoDataDatabase.GetCollection<ViewDocument>("views");
            _savedQueries = discoDataDatabase.GetCollection<SavedQueryDocument>("savedQueries");
            _publishedDatasets = discoDataDatabase.GetCollection<PublishedDatasetDocument>("publishedDatasets");
        }

        /// <summary>
        /// Get all documents from the collection
        /// </summary>
        /// <returns></returns>
        public async Task<List<MongoPublicDocument>> GetAllAsync()
        {
            try
            {
                List<MongoDocument> result = await _collection.Find(p => p.IsActive).ToListAsync();
                return [.. result.Select(item => new MongoPublicDocument(item))];
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting all documents");
                return [];
            }
        }

        /// <summary>
        /// Get document by view UUID 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<MongoPublicDocument> GetById(string id)
        {
            try
            {
                MongoDocument doc = await _collection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync();

                if (doc != null) return  new MongoPublicDocument(doc);
                throw new ViewNotFoundException();

            }
            catch (Exception ex)
            {
#pragma warning disable CA2254 // La plantilla debe ser una expresi�n est�tica
                _logger.LogError(ex, string.Format("Error while getting document with id {0}", id));
#pragma warning restore CA2254 // La plantilla debe ser una expresi�n est�tica
                throw;
            }
        }

        /// <summary>
        /// Get document by view UUID with all fields
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<MongoDocument?> GetFullDocumentById(string id)
        {
            try
            {
                MongoDocument result = await _collection.Find(p => p.IsActive && p._id == id).FirstOrDefaultAsync();
                if (result != null) return result;
                throw new ViewNotFoundException();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with mongo id {id}");
                return null;
            }
        }

        /// <summary>Get all active owners</summary>
        public async Task<List<OwnerDocument>> GetAllOwnersAsync()
        {
            try
            {
                return await _owners.Find(o => o.IsActive).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting owners");
                return [];
            }
        }

        /// <summary>Get all active views for an owner by MongoDB owner ID</summary>
        public async Task<(OwnerDocument? owner, List<ViewDocument> views)> GetViewsByOwnerIdAsync(string ownerId)
        {
            try
            {
                var owner = await _owners.Find(o => o.Id == ownerId && o.IsActive).FirstOrDefaultAsync();
                if (owner == null) return (null, []);

                var views = await _views.Find(v => v.OwnerId == ownerId && v.IsActive).ToListAsync();
                return (owner, views);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting views for owner {OwnerId}", ownerId);
                return (null, []);
            }
        }

        /// <summary>Insert a new view document</summary>
        public async Task<ViewDocument> InsertViewAsync(ViewDocument view)
        {
            await _views.InsertOneAsync(view);
            return view;
        }

        public async Task<bool> UpdateViewAsync(string viewId, string? displayName, string? description)
        {
            var updates = new List<UpdateDefinition<ViewDocument>>();
            if (displayName != null) updates.Add(Builders<ViewDocument>.Update.Set(v => v.DisplayName, displayName));
            if (description != null) updates.Add(Builders<ViewDocument>.Update.Set(v => v.Description, description));
            if (updates.Count == 0) return false;
            var result = await _views.UpdateOneAsync(v => v.Id == viewId, Builders<ViewDocument>.Update.Combine(updates));
            return result.ModifiedCount > 0;
        }

        /// <summary>Soft-delete a view by MongoDB view ID</summary>
        public async Task<bool> DeleteViewAsync(string viewId)
        {
            var result = await _views.UpdateOneAsync(
                v => v.Id == viewId,
                Builders<ViewDocument>.Update.Set(v => v.IsActive, false));
            return result.ModifiedCount > 0;
        }

        /// <summary>Insert a new owner document</summary>
        public async Task<OwnerDocument> InsertOwnerAsync(OwnerDocument owner)
        {
            await _owners.InsertOneAsync(owner);
            return owner;
        }

        /// <summary>Get owner by ID</summary>
        public async Task<OwnerDocument?> GetOwnerByIdAsync(string ownerId)
        {
            try
            {
                return await _owners.Find(o => o.Id == ownerId && o.IsActive).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting owner {OwnerId}", ownerId);
                return null;
            }
        }

        /// <summary>Get owner by name</summary>
        public async Task<OwnerDocument?> GetOwnerByNameAsync(string name)
        {
            return await _owners.Find(o => o.Name == name && o.IsActive).FirstOrDefaultAsync();
        }

        /// <summary>Get a single view by MongoDB view ID</summary>
        public async Task<ViewDocument?> GetViewByIdAsync(string viewId)
        {
            try
            {
                return await _views.Find(v => v.Id == viewId && v.IsActive).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting view {ViewId}", viewId);
                return null;
            }
        }

        // ── Saved Queries ──

        /// <summary>Insert a new saved query</summary>
        public async Task<SavedQueryDocument> InsertSavedQueryAsync(SavedQueryDocument doc)
        {
            await _savedQueries.InsertOneAsync(doc);
            return doc;
        }

        /// <summary>Get a saved query by ID</summary>
        public async Task<SavedQueryDocument?> GetSavedQueryByIdAsync(string id)
        {
            try
            {
                return await _savedQueries.Find(q => q.Id == id && q.IsActive).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting saved query {Id}", id);
                return null;
            }
        }

        /// <summary>Get all saved queries for an owner</summary>
        public async Task<List<SavedQueryDocument>> GetSavedQueriesByOwnerAsync(string ownerId)
        {
            try
            {
                return await _savedQueries.Find(q => q.OwnerId == ownerId && q.IsActive).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting saved queries for owner {OwnerId}", ownerId);
                return [];
            }
        }

        /// <summary>Update a saved query's SQL and/or metadata</summary>
        public async Task<bool> UpdateSavedQueryAsync(string id, string? displayName, string? description, string? sql, List<LogicalRef>? logicalRefs)
        {
            var updates = new List<UpdateDefinition<SavedQueryDocument>>();
            if (displayName != null) updates.Add(Builders<SavedQueryDocument>.Update.Set(q => q.DisplayName, displayName));
            if (description != null) updates.Add(Builders<SavedQueryDocument>.Update.Set(q => q.Description, description));
            if (sql != null) updates.Add(Builders<SavedQueryDocument>.Update.Set(q => q.Sql, sql));
            if (logicalRefs != null) updates.Add(Builders<SavedQueryDocument>.Update.Set(q => q.LogicalRefs, logicalRefs));
            if (updates.Count == 0) return false;
            updates.Add(Builders<SavedQueryDocument>.Update.Set(q => q.UpdatedAt, DateTime.UtcNow));
            var result = await _savedQueries.UpdateOneAsync(q => q.Id == id && q.IsActive, Builders<SavedQueryDocument>.Update.Combine(updates));
            return result.ModifiedCount > 0;
        }

        /// <summary>Soft-delete a saved query</summary>
        public async Task<bool> DeleteSavedQueryAsync(string id)
        {
            var result = await _savedQueries.UpdateOneAsync(
                q => q.Id == id,
                Builders<SavedQueryDocument>.Update.Set(q => q.IsActive, false));
            return result.ModifiedCount > 0;
        }

        // ── Published Datasets ──

        /// <summary>List all active published datasets</summary>
        public async Task<List<PublishedDatasetDocument>> GetPublishedDatasetsAsync()
        {
            try
            {
                return await _publishedDatasets.Find(d => d.IsActive).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting published datasets");
                return [];
            }
        }

        /// <summary>Get a published dataset by dataset + table + version</summary>
        public async Task<PublishedDatasetDocument?> GetPublishedDatasetAsync(string dataset, string table, string version = "latest")
        {
            try
            {
                return await _publishedDatasets.Find(d =>
                    d.Dataset == dataset && d.Table == table && d.Version == version && d.IsActive
                ).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting published dataset {Dataset}.{Table}@{Version}", dataset, table, version);
                return null;
            }
        }

        /// <summary>Get all tables for a given dataset (schema)</summary>
        public async Task<List<PublishedDatasetDocument>> GetPublishedTablesByDatasetAsync(string dataset)
        {
            try
            {
                return await _publishedDatasets.Find(d =>
                    d.Dataset == dataset && d.Version == "latest" && d.IsActive
                ).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting published tables for dataset {Dataset}", dataset);
                return [];
            }
        }

        /// <summary>Upsert a published dataset entry (used by the promotion job)</summary>
        public async Task UpsertPublishedDatasetAsync(PublishedDatasetDocument doc)
        {
            var filter = Builders<PublishedDatasetDocument>.Filter.Where(d =>
                d.Dataset == doc.Dataset && d.Table == doc.Table && d.Version == doc.Version);

            var existing = await _publishedDatasets.Find(filter).FirstOrDefaultAsync();
            if (existing != null)
            {
                doc.Id = existing.Id;
                await _publishedDatasets.ReplaceOneAsync(filter, doc);
            }
            else
            {
                doc.Id = MongoDB.Bson.ObjectId.GenerateNewId().ToString();
                await _publishedDatasets.InsertOneAsync(doc);
            }
        }

        /// <summary>Soft-delete a published dataset (unpublish)</summary>
        public async Task<bool> UnpublishDatasetAsync(string dataset, string table, string version)
        {
            var result = await _publishedDatasets.UpdateOneAsync(
                d => d.Dataset == dataset && d.Table == table && d.Version == version,
                Builders<PublishedDatasetDocument>.Update.Set(d => d.IsActive, false));
            return result.ModifiedCount > 0;
        }
    }
}