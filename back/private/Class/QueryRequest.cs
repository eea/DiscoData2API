using System.ComponentModel;

namespace DiscoData2API_Priv.Class
{
    public class QueryRequest
    {
        [DefaultValue(typeof(string[]), "")]
        public string[]? Fields { get; set; }
        [DefaultValue(typeof(string[]), "")]
        public string[]? Filters { get; set; }
        [DefaultValue(150)]
        public int? Limit { get; set; }
    }
}