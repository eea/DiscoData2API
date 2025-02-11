using System.Text.Json.Serialization;

namespace DiscoData2API_Priv.Class
{
    public class DremioJob
    {
        [JsonPropertyName("id")]
        public string? Id { get; set; }

        public string? Query { get; set; }
    }
}