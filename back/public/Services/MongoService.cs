using DiscoData2API.Misc;
using DiscoData2API_Library.Class;
using DiscoData2API_Library.Model;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

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
                  _collection = database.GetCollection<MongoDocument>(MyEnum.Collection.dremio_queries.ToString());
            }

            /// <summary>
            /// Get all documents from the collection
            /// </summary>
            /// <returns></returns
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
            /// Get document by id
            /// </summary>
            /// <param name="id"></param>
            /// <returns></returns>
            public async Task<MongoDocument> GetById(string id)
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
      }
}