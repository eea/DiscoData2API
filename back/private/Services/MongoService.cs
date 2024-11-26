using DiscoData2API_Priv.Misc;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using DiscoData2API_Library.Model;
using DiscoData2API_Library.Class;

namespace DiscoData2API_Priv.Services
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
            _collection = database.GetCollection<MongoDocument>(MyEnum.Collection.discodata_queries.ToString());
        }

        /// <summary>
        /// Get all documents from the collection
        /// </summary>
        /// <returns></returns>
        public async Task<List<MongoDocument>> GetAllAsync()
        {
            try
            {
                return await _collection.Find(_ => true).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting all documents");
                return new List<MongoDocument>();
            }
        }

        /// <summary>
        /// Create a document
        /// </summary>
        /// <param name="document"></param>
        public async Task<MongoDocument> CreateAsync(MongoDocument mongoDocument)
        {
            try
            {
                await _collection.InsertOneAsync(mongoDocument);
                return mongoDocument;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while creating mongoDb document");
                return null;
            }
        }

        /// <summary>
        /// Get document by id
        /// </summary>
        /// <param name="id"></param>
        public async Task<MongoDocument> ReadAsync(string id)
        {
            try
            {
                return await _collection.Find(p => p._id == id).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with id {id}");
                return null;
            }
        }

        /// <summary>
        ///   Update a document by id
        ///   </summary>
        ///   <param name="document"></param>
        public async Task<MongoDocument> UpdateAsync(MongoDocument mongoDocument)
        {
            try
            {
                await _collection.ReplaceOneAsync(p => p._id == mongoDocument._id, mongoDocument);
                return mongoDocument;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while updating document with id {mongoDocument._id}");
                return null;
            }
        }

        /// <summary>
        /// Delete a document by id
        /// </summary>
        /// <param name="id"></param>
        public async Task<bool> DeleteAsync(string id)
        {
            try
            {
                var result = await _collection.DeleteOneAsync(p => p._id == id);
                return result.DeletedCount > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while deleting document with id {id}");
                return false;
            }
        }
    }
}