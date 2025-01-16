namespace DiscoData2API.Model;

using DiscoData2API.Model;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;


[BsonIgnoreExtraElements]
public class MongoDocument : MongoBaseDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string _id { get; set; } = null!;


    [BsonElement("id")]
    public string ID { get; set; } = null!;


    [BsonElement("name")]
    public string Name { get; set; } = null!;

    [BsonElement("description")]
    public string Description { get; set; } = null!;


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

public class MongoPublicDocument
{
    public string ID { get; set; } = null!;

    public string Name { get; set; } = null!;

    public string Description { get; set; } = null!;

    public Boolean IsActive { get; set; } = false;

    public string? Version { get; set; } = null;

    public DateTime? Date { get; set; } = null;

    private string Query { get; set; } = null!;

    public List<Field>? Fields { get; set; } = null!;

    public MongoPublicDocument()
    {
    }

    public MongoPublicDocument(MongoDocument doc)
    {
        ID = doc.ID;
        Name = doc.Name;
        Description = doc.Description;
        IsActive = doc.IsActive;
        Version = doc.Version;
        Date = doc.Date;
        Query = doc.Query;
        Fields = doc.Fields;
    }


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