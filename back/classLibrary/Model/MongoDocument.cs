namespace DiscoData2API_Library.Model;

using System.ComponentModel;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;


[BsonIgnoreExtraElements]
public class MongoDocument : MongoBaseDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    [DefaultValue("67408ccc9a5be59035cfd32f")]
    public string? _id { get; set; } = null!;

    [BsonElement("name")]
    [DefaultValue("Hello world query")]
    public string Name { get; set; } = null!;

    [BsonElement("version")]
    [DefaultValue("V1")]
    public string? Version { get; set; }

    [BsonElement("date")]
    public DateTime Date { get; set; }

    [BsonElement("query")]
    [DefaultValue("SELECT * FROM \"Local S3\".\"datahub-pre-01\".discodata.CO2_emissions.latest.co2cars LIMIT 20")]
    public string Query { get; set; } = null!;

    [BsonElement("fields")]
    public List<Field>? Fields { get; set; } = null!;
}

public class Field
{
    [BsonElement("name")]
    [DefaultValue("Mp")]
    public string Name { get; set; } = null!;

    [BsonElement("type")]
    [DefaultValue("string")]
    public string Type { get; set; } = null!;

    [BsonElement("description")]
    [DefaultValue("Mp is the constructor barand")]
    public string? Description { get; set; } = null!;
}