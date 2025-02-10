using Microsoft.Extensions.Options;
using MongoDB.Driver;
using DiscoData2API_Priv.Model;
using DiscoData2API_Priv.Class;
using DiscoData2API_Priv.Misc;

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
            catch
            {
                throw;
            }
        }

        public async Task<List<MongoDocument>> GetAllByUserAsync(string userAdded)
        {
            try
            {
                return await _collection.Find(p => p.IsActive && p.UserAdded == userAdded).ToListAsync();
            }
            catch
            {
                throw;
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
        /// <param name="id">UUID of the view</param>
        /// <param name="exception">Throw exception if ID does not exist. true by default</param>
        public async Task<MongoDocument?> ReadAsync(string id, bool exception=true)
        {
            try
            {
                MongoDocument doc= await _collection.Find(p => p.ID == id && p.IsActive).FirstOrDefaultAsync();
                if (doc!=null) return doc;
                if (exception) throw new ViewNotFoundException();
                return null;

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with id {id}");
                throw;
            }
        }

        /// <summary>
        /// Get document by MongoId
        /// </summary>
        /// <param name="_id">MongoID</param>
        public async Task<MongoDocument?> ReadByMongoIDAsync(string _id)
        {
            try
            {
                MongoDocument doc = await _collection.Find(p => p._id == _id && p.IsActive).FirstOrDefaultAsync();
                if (doc != null) return doc;
                throw new ViewNotFoundException();

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with id {_id}");
                throw;
            }
        }



        /// <summary>
        /// Update a document by id
        /// </summary>
        /// <param name="id">The Id of the document to update</param>
        /// <param name="newDocument">What to change. Only pass what you wanna change</param>
        /// <param name="MongoDB_id">True if id is MongoDB id</param>
        /// <returns></returns>
        public async Task<MongoDocument?> UpdateAsync(string id, MongoDocument newDocument, bool MongoDB_id=false)
        {
            try
            {
                // Fetch the existing document
                MongoDocument myDocument = MongoDB_id ?
                    await _collection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync() :
                    await _collection.Find(p => p.ID == id && p.IsActive).FirstOrDefaultAsync();

                if (myDocument == null)
                {
                    _logger.LogWarning($"Document with id {id} not found or inactive.");
                    return null;
                }

                // Update only the provided fields
                myDocument.Name = !string.IsNullOrEmpty(newDocument.Name) ? newDocument.Name : myDocument.Name;
                myDocument.Description = !string.IsNullOrEmpty(newDocument.Description) ? newDocument.Description : myDocument.Description;
                myDocument.Query = !string.IsNullOrEmpty(newDocument.Query) ? newDocument.Query : myDocument.Query;
                myDocument.Fields = newDocument.Fields ?? myDocument.Fields;
                myDocument.Version = !string.IsNullOrEmpty(newDocument.Version) ? newDocument.Version : myDocument.Version;
                myDocument.UserAdded = !string.IsNullOrEmpty(newDocument.UserAdded) ? newDocument.UserAdded : myDocument.UserAdded;
                myDocument.Date = newDocument.Date ?? myDocument.Date;
                myDocument.ID = newDocument.ID;

                // Replace the updated document
                if (MongoDB_id) 
                    await _collection.ReplaceOneAsync(p => p._id == id, myDocument);
                else
                    await _collection.ReplaceOneAsync(p => p.ID == id, myDocument);

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
                var doc= await _collection.Find(p => p.ID == id && p.IsActive).FirstOrDefaultAsync();
                if (doc != null)
                {
                    var result = await _collection.UpdateOneAsync(p => p.ID == id, Builders<MongoDocument>.Update.Set(p => p.IsActive, false));

                    return result.ModifiedCount > 0;
                }
                return false;
            }

            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while deleting document with id {id}");
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
                MongoDocument result = await _collection.Find(p => p.IsActive && p.ID == id).FirstOrDefaultAsync();
                if (result != null) return result;
                throw new ViewNotFoundException();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with mongo id {id}");
                throw;
            }
        }


    }

}
