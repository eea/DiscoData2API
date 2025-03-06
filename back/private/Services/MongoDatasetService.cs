using DiscoData2API_Priv.Class;
using DiscoData2API_Priv.Model;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace DiscoData2API_Priv.Services
{
    public class MongoDatasetService
    {

        private readonly ILogger<MongoService> _logger;
        private readonly IMongoCollection<MongoDatasetDocument> _collection;
        public MongoDatasetService(IOptions<ConnectionSettingsMongo> mongoSettings, ILogger<MongoService> logger)
        {
            _logger = logger;
            var mongoClient = new MongoClient(mongoSettings.Value.ConnectionString);
            var database = mongoClient.GetDatabase(mongoSettings.Value.DatabaseName);
            _collection = database.GetCollection<MongoDatasetDocument>("datasets");
        }

        /// <summary>
        /// Get all documents from the collection
        /// </summary>
        /// <returns></returns>
        public async Task<List<MongoDatasetDocument>> GetAllAsync()
        {
            try
            {
                return await _collection.Find(p=>  p.ID!= "0b8b78a3-24f0-42d7-9ed3-f8e69d4089e6").ToListAsync();
            }
            catch
            {
                throw;
            }
        }


        public async Task Update(MongoDatasetDocument datasetDocument)
        {
            
            await _collection.ReplaceOneAsync(p => p.ID == datasetDocument.ID, datasetDocument);

        }

    }
}
