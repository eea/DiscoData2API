using Microsoft.Extensions.Options;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Text.RegularExpressions;
using DiscoData2API_Priv.Model;
using DiscoData2API_Priv.Class;
using DiscoData2API_Priv.Misc;

namespace DiscoData2API_Priv.Services
{
    public class MongoService
    {
        private readonly ILogger<MongoService> _logger;
        private readonly IMongoCollection<MongoDocument> _collection;
        private readonly IMongoCollection<MongoProject> _projectCollection;
        private readonly IMongoCollection<MongoUser> _userCollection;
        public MongoService(IOptions<ConnectionSettingsMongo> mongoSettings, ILogger<MongoService> logger)
        {
            _logger = logger;
            var mongoClient = new MongoClient(mongoSettings.Value.ConnectionString);
            var database = mongoClient.GetDatabase(mongoSettings.Value.DatabaseName);
            _collection = database.GetCollection<MongoDocument>(mongoSettings.Value.CollectionName);
            _projectCollection = database.GetCollection<MongoProject>("discodata_project");
            _userCollection = database.GetCollection<MongoUser>("discodata_user");
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
                return await _collection.Find(p => p.IsActive && p.UserAdded == userAdded).SortByDescending(p => p.Date).ToListAsync();
            }
            catch
            {
                throw;
            }
        }

        public async Task<List<MongoDocument>> GetAllByCatalogAsync(string catalog)
        {
            try
            {
                var filter = Builders<MongoDocument>.Filter.And(
                    Builders<MongoDocument>.Filter.Eq(p => p.IsActive, true),
                    Builders<MongoDocument>.Filter.Regex(p => p.Catalog, new BsonRegularExpression($"^{Regex.Escape(catalog)}$", "i"))
                );
                return await _collection.Find(filter).SortByDescending(p => p.Date).ToListAsync();
            }
            catch
            {
                throw;
            }
        }

        public async Task<List<MongoDocument>> GetAllByUserAndCatalogAsync(string userAdded, string catalog)
        {
            try
            {
                var filter = Builders<MongoDocument>.Filter.And(
                    Builders<MongoDocument>.Filter.Eq(p => p.IsActive, true),
                    Builders<MongoDocument>.Filter.Eq(p => p.UserAdded, userAdded),
                    Builders<MongoDocument>.Filter.Regex(p => p.Catalog, new BsonRegularExpression($"^{Regex.Escape(catalog)}$", "i"))
                );
                return await _collection.Find(filter).SortByDescending(p => p.Date).ToListAsync();
            }
            catch
            {
                throw;
            }
        }

        public async Task<List<MongoDocument>> GetAllByProjectAsync(string projectId, string? userAdded = null)
        {
            try
            {
                var builder = Builders<MongoDocument>.Filter;
                var filter = builder.And(
                    builder.Eq(p => p.IsActive, true),
                    builder.Eq(p => p.ProjectId, projectId)
                );

                if (!string.IsNullOrEmpty(userAdded))
                {
                    filter = builder.And(filter, builder.Eq(p => p.UserAdded, userAdded));
                }

                return await _collection.Find(filter).SortByDescending(p => p.Date).ToListAsync();
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
                MongoDocument doc= await _collection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync();
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
        /// <param name="has_uuid">False if id is MongoDB id</param>
        /// <returns></returns>
        public async Task<MongoDocument?> UpdateAsync(string id, MongoDocument newDocument, bool has_uuid=true)
        {
            try
            {
                // Fetch the existing document
                MongoDocument myDocument = has_uuid ?
                    await _collection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync() :
                    await _collection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync();

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
                myDocument.Catalog = !string.IsNullOrEmpty(newDocument.Catalog) ? newDocument.Catalog : myDocument.Catalog;
                myDocument.ProjectId = !string.IsNullOrEmpty(newDocument.ProjectId) ? newDocument.ProjectId : myDocument.ProjectId;

                // Replace the updated document
                if (has_uuid)
                    await _collection.ReplaceOneAsync(p => p._id == id, myDocument);
                else
                    await _collection.ReplaceOneAsync(p => p._id == id, myDocument);

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
                var doc= await _collection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync();
                if (doc != null)
                {
                    var result = await _collection.UpdateOneAsync(p => p._id == id, Builders<MongoDocument>.Update.Set(p => p.IsActive, false));

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
                MongoDocument result = await _collection.Find(p => p.IsActive && p._id == id).FirstOrDefaultAsync();
                if (result != null) return result;
                throw new ViewNotFoundException();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting document with id {id}");
                throw;
            }
        }

        /// <summary>
        /// Bulk update all documents to add catalog field with default value
        /// </summary>
        /// <param name="catalogValue">The catalog value to set for all existing documents</param>
        /// <returns>Number of documents updated</returns>
        public async Task<long> BulkUpdateCatalogAsync(string catalogValue)
        {
            try
            {
                // Update all active documents that don't have a catalog field or have null/empty catalog
                var filter = Builders<MongoDocument>.Filter.And(
                    Builders<MongoDocument>.Filter.Eq(p => p.IsActive, true),
                    Builders<MongoDocument>.Filter.Or(
                        Builders<MongoDocument>.Filter.Exists(p => p.Catalog, false),
                        Builders<MongoDocument>.Filter.Eq(p => p.Catalog, null),
                        Builders<MongoDocument>.Filter.Eq(p => p.Catalog, "")
                    )
                );

                var update = Builders<MongoDocument>.Update.Set(p => p.Catalog, catalogValue);
                var result = await _collection.UpdateManyAsync(filter, update);

                _logger.LogInformation($"Updated {result.ModifiedCount} documents with catalog value: {catalogValue}");
                return result.ModifiedCount;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while bulk updating catalog field to {catalogValue}");
                throw;
            }
        }

        /// <summary>
        /// Remove the 'id' field from all documents in the collection
        /// </summary>
        /// <returns>Number of documents updated</returns>
        public async Task<long> BulkRemoveIdFieldAsync()
        {
            try
            {
                // Find all documents that have an 'id' field
                var filter = Builders<MongoDocument>.Filter.Exists("id", true);

                // Remove the 'id' field from all matching documents
                var update = Builders<MongoDocument>.Update.Unset("id");
                var result = await _collection.UpdateManyAsync(filter, update);

                _logger.LogInformation($"Removed 'id' field from {result.ModifiedCount} documents");
                return result.ModifiedCount;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while removing 'id' field from documents");
                throw;
            }
        }

        #region Project Operations

        /// <summary>
        /// Get all projects from the collection
        /// </summary>
        /// <returns></returns>
        public async Task<List<MongoProject>> GetAllProjectsAsync()
        {
            try
            {
                return await _projectCollection.Find(p => p.IsActive).SortByDescending(p => p.CreationDate).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting all projects");
                throw;
            }
        }

        public async Task<List<MongoProject>> GetAllProjectsByUserAsync(string userAdded)
        {
            try
            {
                return await _projectCollection.Find(p => p.IsActive && p.UserAdded == userAdded).SortByDescending(p => p.CreationDate).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting projects by user {userAdded}");
                throw;
            }
        }

        /// <summary>
        /// Create a project
        /// </summary>
        /// <param name="project"></param>
        public async Task<MongoProject?> CreateProjectAsync(MongoProject project)
        {
            try
            {
                await _projectCollection.InsertOneAsync(project);
                return project;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while creating project");
                return null;
            }
        }

        /// <summary>
        /// Get project by id
        /// </summary>
        /// <param name="id">UUID of the project</param>
        /// <param name="exception">Throw exception if ID does not exist. true by default</param>
        public async Task<MongoProject?> ReadProjectAsync(string id, bool exception = true)
        {
            try
            {
                MongoProject project = await _projectCollection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync();
                if (project != null) return project;
                if (exception) throw new ViewNotFoundException();
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting project with id {id}");
                throw;
            }
        }


        /// <summary>
        /// Update a project by MongoDB ObjectId
        /// </summary>
        /// <param name="id">The MongoDB ObjectId of the project to update</param>
        /// <param name="newProject">What to change. Only pass what you wanna change</param>
        /// <returns></returns>
        public async Task<MongoProject?> UpdateProjectAsync(string id, MongoProject newProject)
        {
            try
            {
                MongoProject existingProject = await _projectCollection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync();

                if (existingProject == null)
                {
                    _logger.LogWarning($"Project with id {id} not found or inactive.");
                    return null;
                }

                existingProject.ProjectName = !string.IsNullOrEmpty(newProject.ProjectName) ? newProject.ProjectName : existingProject.ProjectName;
                existingProject.ProjectDescription = newProject.ProjectDescription ?? existingProject.ProjectDescription;
                existingProject.UserAdded = !string.IsNullOrEmpty(newProject.UserAdded) ? newProject.UserAdded : existingProject.UserAdded;
                existingProject.CreationDate = DateTime.Now;

                await _projectCollection.ReplaceOneAsync(p => p._id == id, existingProject);

                return existingProject;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while updating project with id {id}");
                return null;
            }
        }

        /// <summary>
        /// Delete a project by MongoDB ObjectId
        /// </summary>
        /// <param name="id">The MongoDB ObjectId</param>
        public async Task<bool> DeleteProjectAsync(string id)
        {
            try
            {
                var project = await _projectCollection.Find(p => p._id == id && p.IsActive).FirstOrDefaultAsync();
                if (project != null)
                {
                    var result = await _projectCollection.UpdateOneAsync(p => p._id == id, Builders<MongoProject>.Update.Set(p => p.IsActive, false));
                    return result.ModifiedCount > 0;
                }
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while deleting project with id {id}");
                throw;
            }
        }

        #endregion

        #region User Operations

        public async Task<List<MongoUser>> GetAllUsersAsync()
        {
            try
            {
                var filter = Builders<MongoUser>.Filter.Or(
                    Builders<MongoUser>.Filter.Eq(u => u.IsActive, true),
                    Builders<MongoUser>.Filter.Exists(u => u.IsActive, false)
                );
                return await _userCollection.Find(filter).SortBy(u => u.UserName).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while getting all users");
                throw;
            }
        }

        public async Task<MongoUser?> CreateUserAsync(MongoUser user)
        {
            try
            {
                var existingUser = await _userCollection.Find(u => u.UserName == user.UserName && u.IsActive).FirstOrDefaultAsync();
                if (existingUser != null)
                {
                    _logger.LogWarning($"User with username {user.UserName} already exists");
                    return null;
                }

                await _userCollection.InsertOneAsync(user);
                return user;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while creating user");
                return null;
            }
        }

        public async Task<MongoUser?> ReadUserAsync(string id, bool exception = true)
        {
            try
            {
                var filter = Builders<MongoUser>.Filter.And(
                    Builders<MongoUser>.Filter.Eq(u => u._id, id),
                    Builders<MongoUser>.Filter.Or(
                        Builders<MongoUser>.Filter.Eq(u => u.IsActive, true),
                        Builders<MongoUser>.Filter.Exists(u => u.IsActive, false)
                    )
                );

                MongoUser user = await _userCollection.Find(filter).FirstOrDefaultAsync();
                if (user != null) return user;
                if (exception) throw new ViewNotFoundException();
                return null;
            }
            catch (Exception ex)
            {
                if (exception) throw;
                _logger.LogError(ex, $"Error while reading user with id {id}");
                return null;
            }
        }

        public async Task<MongoUser?> ReadUserByUsernameAsync(string username, bool exception = true)
        {
            try
            {
                var filter = Builders<MongoUser>.Filter.And(
                    Builders<MongoUser>.Filter.Regex(u => u.UserName, new MongoDB.Bson.BsonRegularExpression($"^{System.Text.RegularExpressions.Regex.Escape(username)}$", "i")),
                    Builders<MongoUser>.Filter.Or(
                        Builders<MongoUser>.Filter.Eq(u => u.IsActive, true),
                        Builders<MongoUser>.Filter.Exists(u => u.IsActive, false)
                    )
                );

                MongoUser user = await _userCollection.Find(filter).FirstOrDefaultAsync();
                if (user != null) return user;
                if (exception) throw new ViewNotFoundException();
                return null;
            }
            catch (Exception ex)
            {
                if (exception) throw;
                _logger.LogError(ex, $"Error while reading user with username {username}");
                return null;
            }
        }

        public async Task<MongoUser?> UpdateUserAsync(string id, MongoUser newUser)
        {
            try
            {
                MongoUser existingUser = await _userCollection.Find(u => u._id == id && u.IsActive).FirstOrDefaultAsync();

                if (existingUser == null)
                {
                    _logger.LogWarning($"User with id {id} not found or inactive.");
                    return null;
                }

                newUser._id = existingUser._id;
                newUser.CreatedAt = existingUser.CreatedAt;
                newUser.LastUpdated = DateTime.Now;

                var result = await _userCollection.ReplaceOneAsync(u => u._id == id, newUser);
                return result.ModifiedCount > 0 ? newUser : null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while updating user with id {id}");
                throw;
            }
        }

        public async Task<bool> DeleteUserAsync(string id)
        {
            try
            {
                var user = await _userCollection.Find(u => u._id == id && u.IsActive).FirstOrDefaultAsync();
                if (user != null)
                {
                    var result = await _userCollection.UpdateOneAsync(u => u._id == id, Builders<MongoUser>.Update.Set(u => u.IsActive, false));
                    return result.ModifiedCount > 0;
                }
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while deleting user with id {id}");
                throw;
            }
        }

        public async Task<bool> AddProjectAccessAsync(string userId, string projectId)
        {
            try
            {
                var user = await _userCollection.Find(u => u._id == userId && u.IsActive).FirstOrDefaultAsync();
                if (user == null) return false;

                var project = await _projectCollection.Find(p => p._id == projectId && p.IsActive).FirstOrDefaultAsync();
                if (project == null) return false;

                if (user.UserAccess.Any(ua => ua.ProjectId == projectId)) return true;

                user.UserAccess.Add(new UserProjectAccess { ProjectId = projectId });
                user.LastUpdated = DateTime.Now;

                var result = await _userCollection.ReplaceOneAsync(u => u._id == userId, user);
                return result.ModifiedCount > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while adding project access for user {userId} to project {projectId}");
                throw;
            }
        }

        public async Task<bool> RemoveProjectAccessAsync(string userId, string projectId)
        {
            try
            {
                var user = await _userCollection.Find(u => u._id == userId && u.IsActive).FirstOrDefaultAsync();
                if (user == null) return false;

                var accessToRemove = user.UserAccess.FirstOrDefault(ua => ua.ProjectId == projectId);
                if (accessToRemove == null) return true;

                user.UserAccess.Remove(accessToRemove);
                user.LastUpdated = DateTime.Now;

                var result = await _userCollection.ReplaceOneAsync(u => u._id == userId, user);
                return result.ModifiedCount > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while removing project access for user {userId} from project {projectId}");
                throw;
            }
        }

        public async Task<List<MongoProject>> GetUserProjectsAsync(string userId)
        {
            try
            {
                var userFilter = Builders<MongoUser>.Filter.And(
                    Builders<MongoUser>.Filter.Eq(u => u._id, userId),
                    Builders<MongoUser>.Filter.Or(
                        Builders<MongoUser>.Filter.Eq(u => u.IsActive, true),
                        Builders<MongoUser>.Filter.Exists(u => u.IsActive, false)
                    )
                );

                var user = await _userCollection.Find(userFilter).FirstOrDefaultAsync();
                if (user == null) return new List<MongoProject>();

                var projectIds = user.UserAccess?.Select(ua => ua.ProjectId).ToList() ?? new List<string>();
                if (!projectIds.Any()) return new List<MongoProject>();

                var filter = Builders<MongoProject>.Filter.And(
                    Builders<MongoProject>.Filter.In(p => p._id, projectIds),
                    Builders<MongoProject>.Filter.Eq(p => p.IsActive, true)
                );

                return await _projectCollection.Find(filter).SortBy(p => p.ProjectName).ToListAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while getting projects for user {userId}");
                throw;
            }
        }

        #endregion

    }

}