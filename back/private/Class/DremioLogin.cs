using System.Text.Json.Serialization;

namespace DiscoData2API_Priv.Class
{
    public class DremioLogin
    {
        [JsonPropertyName("token")]
        public string? Token { get; set; }
    }
}