using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace DiscoData2API.Model
{
    [BsonIgnoreExtraElements]
    public class SavedQueryDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = null!;

        [BsonElement("ownerId")]
        [BsonRepresentation(BsonType.ObjectId)]
        public string OwnerId { get; set; } = null!;

        [BsonElement("displayName")]
        public string? DisplayName { get; set; }

        [BsonElement("description")]
        public string? Description { get; set; }

        [BsonElement("sql")]
        public string Sql { get; set; } = null!;

        [BsonElement("logicalRefs")]
        public List<LogicalRef>? LogicalRefs { get; set; }

        [BsonElement("isActive")]
        public bool IsActive { get; set; } = true;

        [BsonElement("createdAt")]
        public DateTime CreatedAt { get; set; }

        [BsonElement("updatedAt")]
        public DateTime UpdatedAt { get; set; }
    }

    public class LogicalRef
    {
        [BsonElement("dataset")]
        public string Dataset { get; set; } = null!;

        [BsonElement("table")]
        public string Table { get; set; } = null!;

        [BsonElement("version")]
        public string? Version { get; set; }
    }
}
