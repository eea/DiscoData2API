namespace DiscoData2API_Library.Class;

using System.Text.Json.Serialization;

public class DremioLogin
{
    [JsonPropertyName("token")]
    public string? Token { get; set; }
}