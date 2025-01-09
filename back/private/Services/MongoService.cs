using Microsoft.Extensions.Options;
using MongoDB.Driver;
using DiscoData2API_Priv.Model;
using DiscoData2API_Priv.Class;

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
            _collection = database.GetCollection<MongoDocument>(mongoSettings.Value.CollectionName);
        }

        /// <summary>
        /// Get all documents from the collection
        /// </summary>
        /// <returns></returns>
        public async Task<List<MongoDocument>> GetAllAsync()
        {
            try
            {
                return await _collection.Find(p => p.IsActive).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting all documents");
                return new List<MongoDocument>();
            }
        }

        public async Task<List<MongoDocument>> GetAllByUserAsync(string userAdded)
        {
            try
            {
                return await _collection.Find(p => p.IsActive && p.UserAdded == userAdded).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting documents for a specific user");
                return new List<MongoDocument>();
            }
        }

        /// <summary>
        /// Create a document
        /// </summary>
        /// <param name="mongoDocument"></param>
        public async Task<MongoDocument?> CreateAsync(MongoDocument mongoDocument)
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
        public async Task<MongoDocument?> ReadAsync(string id)
        {
            try
            {
                return await _collection.Find(p => p.Id == id && p.IsActive).FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with id {id}");
                return null;
            }
        }

        /// <summary>
        /// Update a document by id
        /// </summary>
        /// <param name="id">The Id of the document to update</param>
        /// <param name="newDocument">What to change. Only pass what you wanna change</param>
        /// <returns></returns>
        public async Task<MongoDocument?> UpdateAsync(string id, MongoDocument newDocument)
        {
            try
            {
                // Fetch the existing document
                var myDocument = await _collection.Find(p => p.Id == id && p.IsActive).FirstOrDefaultAsync();

                if (myDocument == null)
                {
                    _logger.LogWarning($"Document with id {id} not found or inactive.");
                    return null;
                }

                // Update only the provided fields
                myDocument.Name = !string.IsNullOrEmpty(newDocument.Name) ? newDocument.Name : myDocument.Name;
                myDocument.Query = !string.IsNullOrEmpty(newDocument.Query) ? newDocument.Query : myDocument.Query;
                myDocument.Fields = newDocument.Fields ?? myDocument.Fields;
                myDocument.Version = !string.IsNullOrEmpty(newDocument.Version) ? newDocument.Version : myDocument.Version;
                myDocument.Date = newDocument.Date ?? myDocument.Date;

                // Replace the updated document
                await _collection.ReplaceOneAsync(p => p.Id == id, myDocument);

                return myDocument;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while updating document with id {id}");
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
                var result = await _collection.UpdateOneAsync(p => p.Id == id, Builders<MongoDocument>.Update.Set(p => p.IsActive, false));
                return result.ModifiedCount > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while deleting document with id {id}");
                return false;
            }
        }
    }
}