using System.Text.Json.Serialization;

namespace DiscoData2API_Priv.Model
{
    public class DremioTable
    {
      [JsonPropertyName("TABLE_NAME")]
      public string TableName { get; set; }
    }
}