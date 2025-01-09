namespace DiscoData2API.Model;

using DiscoData2API.Model;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;


[BsonIgnoreExtraElements]
public class MongoDocument
{
    [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string _id { get; set; } = null!;

        [BsonElement("id")]
        public string Id { get; set; } = null!;

        [BsonElement("name")]
        public string Name { get; set; } = null!;

        [BsonElement("description")]
        public string Description { get; set; } = null!;

        [BsonElement("userAdded")]
        public string? UserAdded { get; set; } = null!;

        [BsonElement("isActive")]
        public Boolean IsActive { get; set; }

        [BsonElement("version")]
        public string? Version { get; set; }

        [BsonElement("date")]
        public DateTime? Date { get; set; }

        [BsonElement("query")]
        public string Query { get; set; } = null!;

        [BsonElement("fields")]
        public List<Field>? Fields { get; set; } = null!;

}

public class Field
{
    [BsonElement("name")]
    public string Name { get; set; } = null!;

    [BsonElement("type")]
    public string Type { get; set; } = null!;

    [BsonElement("isNullable")]
    public bool IsNullable { get; set; } = true!;

    [BsonElement("columnSize")]
    public string? ColumnSize { get; set; }

    [BsonIgnoreIfNull]
    [BsonElement("description")]
    public string Description { get; set; } = null!;
}