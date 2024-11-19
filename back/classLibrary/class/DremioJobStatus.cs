namespace DiscoData2API_Library;
using System.Text.Json.Serialization;

public class DremioJobStatus
{
    [JsonPropertyName("jobState")]
    public string? JobState { get; set; }
}
