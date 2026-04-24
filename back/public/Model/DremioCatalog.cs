using System.Text.Json.Serialization;

namespace DiscoData2API.Model
{
    public class DremioCatalogContainer
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = null!;

        [JsonPropertyName("children")]
        public List<DremioCatalogItem> Children { get; set; } = [];
    }

    public class DremioCatalogItem
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = null!;

        [JsonPropertyName("path")]
        public List<string> Path { get; set; } = [];

        [JsonPropertyName("type")]
        public string Type { get; set; } = null!;

        [JsonPropertyName("datasetType")]
        public string? DatasetType { get; set; }
    }

    public class DremioCatalogDataset
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = null!;

        [JsonPropertyName("path")]
        public List<string> Path { get; set; } = [];

        [JsonPropertyName("tag")]
        public string Tag { get; set; } = null!;

        [JsonPropertyName("sql")]
        public string Sql { get; set; } = null!;

        [JsonPropertyName("fields")]
        public List<DremioCatalogField> Fields { get; set; } = [];
    }

    public class DremioCatalogField
    {
        [JsonPropertyName("name")]
        public string Name { get; set; } = null!;

        [JsonPropertyName("type")]
        public DremioCatalogFieldType Type { get; set; } = null!;
    }

    public class DremioCatalogFieldType
    {
        [JsonPropertyName("name")]
        public string Name { get; set; } = null!;
    }
}
