using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace DiscoData2API_Priv.Model
{
    [BsonIgnoreExtraElements]
    public class MongoProject
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string _id { get; set; } = null!;

        [BsonElement("projectName")]
        public string ProjectName { get; set; } = null!;

        [BsonElement("projectDescription")]
        public string? ProjectDescription { get; set; }

        [BsonElement("userAdded")]
        public string? UserAdded { get; set; }

        [BsonElement("isActive")]
        public bool IsActive { get; set; } = true;

        [BsonElement("creationDate")]
        public DateTime CreationDate { get; set; } = DateTime.Now;
    }

    public class MongoProjectBaseDocument
    {
        public required string ProjectName { get; set; }
        public string? ProjectDescription { get; set; }
        public string? UserAdded { get; set; }
    }

    [BsonIgnoreExtraElements]
    public class MongoUser
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string _id { get; set; } = null!;

        [BsonElement("UserName")]
        public string UserName { get; set; } = null!;

        [BsonElement("userAccess")]
        public List<UserProjectAccess> UserAccess { get; set; } = new List<UserProjectAccess>();

        [BsonElement("createdAt")]
        public DateTime CreatedAt { get; set; } = DateTime.Now;

        [BsonElement("lastUpdated")]
        public DateTime LastUpdated { get; set; } = DateTime.Now;

        [BsonElement("isActive")]
        public bool IsActive { get; set; } = true;
    }

    public class UserProjectAccess
    {
        [BsonElement("projectId")]
        [BsonRepresentation(BsonType.ObjectId)]
        public string ProjectId { get; set; } = null!;
    }

    public class MongoUserBaseDocument
    {
        public required string UserName { get; set; }
        public List<UserProjectAccess>? UserAccess { get; set; }
    }

    public class UserProjectAccessRequest
    {
        public required string UserId { get; set; }
        public required string ProjectId { get; set; }
    }
}