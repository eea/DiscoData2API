using System.Text.Json.Serialization;

namespace DiscoData2API_Priv.Model
{
    public class DremioSchema
    {
      [JsonPropertyName("TABLE_SCHEMA")]
      public string TableSchema { get; set; }

      public string SchemaName { get; set; }
    }
}