using System.ComponentModel;
using System.Linq;
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
            StringBuilder filter_query = new();

            // Concatenate filters 
            filter_query.AppendFormat(" {0} (", Concat);
            filter_query.AppendFormat("{0} ", FieldName);

            filter_query.AppendFormat("{0} ", Condition);

            switch (Condition.ToUpper().Trim())
            {
                case "BETWEEN":
                    filter_query.AppendFormat("{0} ", string.Join(" AND ", Values));
                    break;
                case "IN":
                    filter_query.AppendFormat("( {0} )", string.Join(", ", Values));
                    break;
                default:
                    filter_query.Append(string.Join(", ", Values));
                    break;
            }
            filter_query.Append(") ");

            return filter_query.ToString();
        }

    }

    /// <summary>
    /// Unified request class for executing queries with optional parameters, filters, and fields selection
    /// </summary>
    public class QueryExecutionRequest
    {
        /// <summary>
        /// Query parameters for substitution in parameterized queries
        /// </summary>
        [DefaultValue(typeof(Dictionary<string, object>), "")]
        public Dictionary<string, object>? Parameters { get; set; }

        /// <summary>
        /// Fields to select from the query results
        /// </summary>
        [DefaultValue(typeof(string[]), "")]
        public string[]? Fields { get; set; }

        /// <summary>
        /// Additional filters to apply to the query
        /// </summary>
        [DefaultValue(typeof(FilterDefinition[]), "")]
        public FilterDefinition[]? Filters { get; set; }

        /// <summary>
        /// Maximum number of rows to return
        /// </summary>
        [DefaultValue(150)]
        public int? Limit { get; set; }
    }


}