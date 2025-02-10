using System.Text.Json.Serialization;

namespace DiscoData2API_Priv.Model;

public class DremioColumn
{
      [JsonPropertyName("COLUMN_NAME")]
      public string COLUMN_NAME { get; set; } = null!;
      [JsonPropertyName("COLUMN_SIZE")]
      public int COLUMN_SIZE { get; set; }
      [JsonPropertyName("NUMERIC_PRECISION")]
      public int NUMERIC_PRECISION { get; set; }
      [JsonPropertyName("IS_NULLABLE")]
      public string IS_NULLABLE { get; set; } = null!;
     [JsonPropertyName("DATA_TYPE")]
      public string DATA_TYPE { get; set; } = null!;

}