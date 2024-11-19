namespace DiscoData2API_Library.Class;

using System.Text.Json.Serialization;

public class DremioJobStatus
{
    [JsonPropertyName("jobState")]
    public string? JobState { get; set; }
}
