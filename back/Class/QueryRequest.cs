namespace DiscoData2API.Class
{
      public class QueryRequest
      {
            public string[] Fields { get; set; }
            public string[] Filters { get; set; }
            public int Limit { get; set; }
            public int Offset { get; set; }
      }
}