using System.Text.Json.Serialization;

namespace DiscoData2API.Class
{
    public class DremioJobStatus
    {
        [JsonPropertyName("jobState")]
        public string? JobState { get; set; }
    }
}