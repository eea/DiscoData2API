using DiscoData2API.Class;

namespace DiscoData2API_Priv.Model
{
    public class QueryExecutionRequest
    {
        public Dictionary<string, string>? Parameters { get; set; }
        public string[]? Fields { get; set; }
        public FilterDefinition[]? Filters { get; set; }
        public int? Limit { get; set; }
    }
}