using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization;

namespace DiscoData2API.Model
{
    [BsonIgnoreExtraElements]
    public class OwnerDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = null!;

        [BsonElement("name")]
        public string Name { get; set; } = null!;

        [BsonElement("isActive")]
        public bool IsActive { get; set; }
    }

    [BsonIgnoreExtraElements]
    public class ViewDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = null!;

        [BsonElement("ownerId")]
        [BsonRepresentation(BsonType.ObjectId)]
        public string OwnerId { get; set; } = null!;

        [BsonElement("path")]
        public string Path { get; set; } = null!;

        public string Name => Path.Split('.').Last();

        [BsonElement("description")]
        public string? Description { get; set; }

        [BsonElement("isActive")]
        public bool IsActive { get; set; }

        [BsonElement("template")]
        public BsonDocument? Template { get; set; }
    }
}
