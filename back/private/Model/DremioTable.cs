using System.Text.Json.Serialization;

namespace DiscoData2API_Priv.Model
{
  public class DremioTable
  {
    public string TableName { get; set; }
  }

  public class DremioRawTable
  {
    public string TABLE_NAME { get; set; }
  }
}