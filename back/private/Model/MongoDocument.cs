using Google.Protobuf.WellKnownTypes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace DiscoData2API_Priv.Model
{
    [BsonIgnoreExtraElements]
    public class MongoDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string _id { get; set; } = null!;

        [BsonElement("id")]
        public string ID { get; set; } = null!;

        [BsonElement("name")]
        public string Name { get; set; } = null!;

        [BsonElement("catalog")]
        public string? Catalog { get; set; }

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

        [BsonElement("parameters")]
        public List<ViewParameter>? Parameters { get; set; } = null!;
    }

    public class Field
    {
        private string? _type = string.Empty;
        private string? _size = string.Empty;

        [BsonElement("name")]
        public string Name { get; set; } = null!;

        [BsonElement("type")]
        public string Type
        {
            get { return string.IsNullOrEmpty(_type) ? string.Empty : _type; }
            set
            {
                _type = "string";
                switch (value.ToUpper())
                {
                    case "CHARACTER":
                    case "CHARACTER VARYING":
                        _type = "string";
                        break;
                    case "INTEGER":
                        _type = "int32";
                        _size = "4";
                        break;
                    case "BIGINT":
                        _type = "int64";
                        _size = "8";
                        break;
                    case "DOUBLE":
                    case "FLOAT":
                    case "FLOAT32":
                    case "FLOAT64":
                    case "DECIMAL":
                    case "DECIMAL128":
                    case "DECIMAL256":
                        _type = "float64";
                        _size = "8";
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
        public string? ColumnSize {
            get { return _size; }
            set
            {
                _size = value;
            }
        }

        [BsonIgnoreIfNull]
        [BsonElement("description")]
        public string Description { get; set; } = null!;
    }

    public class ViewParameter
    {
        [BsonElement("name")]
        public string Name { get; set; } = null!;

        [BsonElement("type")]
        public string Type { get; set; } = "string";

        [BsonElement("description")]
        public string? Description { get; set; }

        [BsonElement("required")]
        public bool Required { get; set; } = true;

        [BsonElement("defaultValue")]
        public string? DefaultValue { get; set; }

        [BsonElement("allowedValues")]
        public List<string>? AllowedValues { get; set; }
    }
}