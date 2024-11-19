namespace DiscoData2API_Library;
using System.Text.Json.Serialization;

public class DremioLogin
{
    [JsonPropertyName("token")]
    public string? Token { get; set; }
}