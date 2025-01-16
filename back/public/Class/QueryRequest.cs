using System.ComponentModel;
using System.Text;

namespace DiscoData2API.Class
{
    public class QueryRequest
    {
        [DefaultValue(typeof(string[]), "")]
        public string[]? Fields { get; set; }
        [DefaultValue(typeof(string[]), "")]
        public FilterDefinition[]? Filters { get; set; }
        [DefaultValue(150)]
        public int? Limit { get; set; }

    }

    public class FilterDefinition
    {
        [DefaultValue(typeof(string), "")]
        public required string FieldName { get; set; }

        [DefaultValue(typeof(string), "")]
        public required string Condition { get; set; }

        [DefaultValue(typeof(string[]), "")]
        public required string[]? Values { get; set; }

        [DefaultValue(typeof(string), "AND")]
        public string Concat { get; set; } = "AND";

        public string BuildFilterString()
        {
            StringBuilder filter_query = new StringBuilder();

            // Concatenate filters 
            filter_query.AppendFormat(" {0} (", Concat);
            filter_query.AppendFormat("{0} ", FieldName);
            filter_query.AppendFormat("{0} {1}",
                Condition,
                string.Join(Condition.ToUpper().Trim() == "BETWEEN" ? " AND " : ",", Values));
            filter_query.Append(") ");


            return filter_query.ToString();
        }

    }


}