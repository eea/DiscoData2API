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

        /// <summary>
        /// Get document by view UUID 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<MongoPublicDocument> GetById(string id)
        {
            try
            {

                MongoDocument result = await _collection.Find(p => p.IsActive && p.ID == id).FirstAsync();
                return new MongoPublicDocument(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with mongo id {id}");
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
                MongoDocument result = await _collection.Find(p => p.IsActive && p.ID == id).FirstAsync();
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with mongo id {id}");
                return null;
            }
        }


    }
}