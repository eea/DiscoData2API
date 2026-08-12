using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace DiscoData2API.Model
{
    [BsonIgnoreExtraElements]
    public class PublishedDatasetDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = null!;

        [BsonElement("dataset")]
        public string Dataset { get; set; } = null!;

        [BsonElement("table")]
        public string Table { get; set; } = null!;

        [BsonElement("version")]
        public string Version { get; set; } = "latest";

        [BsonElement("files")]
        public List<string> Files { get; set; } = [];

        [BsonElement("columns")]
        public List<PublishedColumn> Columns { get; set; } = [];

        [BsonElement("partitionKeys")]
        public List<string>? PartitionKeys { get; set; }

        [BsonElement("sortKeys")]
        public List<string>? SortKeys { get; set; }

        [BsonElement("rowCount")]
        public long RowCount { get; set; }

        [BsonElement("sizeBytes")]
        public long SizeBytes { get; set; }

        [BsonElement("description")]
        public string? Description { get; set; }

        [BsonElement("licence")]
        public string? Licence { get; set; }

        [BsonElement("promotedAt")]
        public DateTime PromotedAt { get; set; }

        [BsonElement("promotedBy")]
        public string? PromotedBy { get; set; }

        [BsonElement("isActive")]
        public bool IsActive { get; set; } = true;
    }

    public class PublishedColumn
    {
        [BsonElement("name")]
        public string Name { get; set; } = null!;

        [BsonElement("type")]
        public string Type { get; set; } = null!;

        [BsonElement("description")]
        public string? Description { get; set; }
    }
}
