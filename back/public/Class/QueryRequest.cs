using System.ComponentModel;

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
        [DefaultValue(0)]
        public int? Offset { get; set; }
        [DefaultValue(typeof(string[]), "")]
        public string[]? GroupBy { get; set; }
        [DefaultValue(typeof(AggregateDefinition[]), "")]
        public AggregateDefinition[]? Aggregates { get; set; }

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

    }

    public class AggregateDefinition
    {
        /// <summary>Aggregate function: COUNT, SUM, AVG, MIN, MAX</summary>
        [DefaultValue("COUNT")]
        public required string Function { get; set; }

        /// <summary>Column to aggregate (use * for COUNT)</summary>
        [DefaultValue("*")]
        public required string Field { get; set; }

        /// <summary>Alias for the result column</summary>
        [DefaultValue("")]
        public string? Alias { get; set; }

        /// <summary>Granularity for DATE_TRUNC: month, year, day, quarter, week</summary>
        [DefaultValue("")]
        public string? Granularity { get; set; }
    }

}