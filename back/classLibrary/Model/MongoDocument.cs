namespace DiscoData2API_Library.Model;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;


[BsonIgnoreExtraElements]
public class MongoDocument : MongoBaseDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string _id { get; set; } = null!;

    [BsonElement("name")]
    public string Name { get; set; } = null!;

    [BsonElement("version")]
    public int Version { get; set; }

    [BsonElement("date")]
    public DateTime Date { get; set; }

    [BsonElement("query")]
    public string Query { get; set; } = null!;

    [BsonElement("fields")]
    public List<Field> Fileds { get; set; } = null!;
}

public class Field
{
    [BsonElement("name")]
    public string Name { get; set; } = null!;

    [BsonElement("type")]
    public string Type { get; set; } = null!;

    [BsonElement("description")]
    public string Description { get; set; } = null!;
}