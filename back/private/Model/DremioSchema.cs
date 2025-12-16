using System.Text.Json.Serialization;

namespace DiscoData2API_Priv.Model
{
  public class DremioSchema
  {
    public string Schema { get; set; }
    public string SchemaName { get; set; }
  }

  public class DremioRawSchema
  {
    public string TABLE_SCHEMA { get; set; }
    public string SchemaName { get; set; }
  }
}