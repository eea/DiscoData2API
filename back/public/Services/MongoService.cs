using DiscoData2API.Misc;
using DiscoData2API.Class;
using DiscoData2API.Model;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Collections.Generic;


namespace DiscoData2API.Services
{
    public class MongoService
    {
        private readonly ILogger<MongoService> _logger;
        private readonly IMongoCollection<MongoDocument> _collection;
        public MongoService(IOptions<ConnectionSettingsMongo> mongoSettings, ILogger<MongoService> logger)
        {
            _logger = logger;
            var mongoClient = new MongoClient(mongoSettings.Value.ConnectionString);
            var database = mongoClient.GetDatabase(mongoSettings.Value.DatabaseName);
            _collection = database.GetCollection<MongoDocument>(mongoSettings.Value.CollectionName);
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
                return result.Select(item => new MongoPublicDocument(item)).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting all documents");
                return new List<MongoPublicDocument>();
            }
        }

        public async Task<List<MongoPublicDocument>> GetAllByUserAsync(string userAdded)
        {
            try
            {
                List<MongoDocument> result = await _collection.Find(p => p.IsActive && p.UserAdded == userAdded).ToListAsync();
                return result.Select(item => new MongoPublicDocument(item)).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting documents for a specific user");
                return new List<MongoPublicDocument>();
            }
        }

        /// <summary>
        /// Get document by view UUID
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<MongoDocument?> ReadAsync(string id)
        {
            try
            {
                return await _collection.Find(p => p.Id == id).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with UUID {id}");
                return null;
            }
        }

    }
}