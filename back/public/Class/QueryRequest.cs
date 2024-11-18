namespace DiscoData2API.Class
{
      public class QueryRequest
      {
        /// <example>1234</example>
        public string[]? Fields { get; set; }
            public string[]? Filters { get; set; }
            public int? Limit { get; set; }
            public int? Offset { get; set; }
      }
}