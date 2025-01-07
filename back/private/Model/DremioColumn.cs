using System.Numerics;

namespace DiscoData2API_Priv.Model;

public class DremioColumn
{
      public string COLUMN_NAME { get; set; } = null!;
      public int COLUMN_SIZE { get; set; }
      public string IS_NULLABLE { get; set; } = null!;
      public string DATA_TYPE { get; set; } = null!;

}