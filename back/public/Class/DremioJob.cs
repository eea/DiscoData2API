using System.Text.Json.Serialization;

namespace DiscoData2API.Class
{
    public class DremioJob
    {
        [JsonPropertyName("id")]
        public string? Id { get; set; }
    }
}