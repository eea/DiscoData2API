namespace DiscoData2API_Priv.Model;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Diagnostics.Eventing.Reader;
using ZstdSharp.Unsafe;

[BsonIgnoreExtraElements]
public class MongoDocument : MongoBaseDocument
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

    private string _type= string.Empty;

     [BsonElement("name")]
     public string Name { get; set; } = null!;

    [BsonElement("type")]
    public string Type
    {
        get {  return _type; }
        set  {
            _type = "string";
            switch (value)
            {
                case "CHARACTER":
                case "CHARACTER VARYING": 
                    _type = "string";
                    break;
                case "INTEGER":
                    _type= "int32";
                    break;
                case "BIGINT":
                    _type = "int64";
                    break;
                case "DOUBLE":
                case "FLOAT":
                case "FLOAT32":
                case "FLOAT64":
                    _type = "float64";
                    break;
                case "geometry":
                    _type = "geometry";
                    break;
                case "BINARY":
                case "BINARY VARYING":
                    _type = "binary";
                    break;

                default:
                    _type = value.ToLower();
                    break;
            }
        }
    }   

    [BsonElement("isNullable")]
    public bool IsNullable { get; set; } = true!;

    [BsonElement("columnSize")]
    public string? ColumnSize { get; set; }

    [BsonIgnoreIfNull]
    [BsonElement("description")]
    public string Description { get; set; } = null!;

}