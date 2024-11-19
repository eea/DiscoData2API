namespace DiscoData2API_Library;
using System.Text.Json.Serialization;

public class DremioJob
{
    [JsonPropertyName("id")]
    public string? Id { get; set; }
}