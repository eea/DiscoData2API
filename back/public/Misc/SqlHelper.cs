using DiscoData2API.Class;
using System.Globalization;

namespace DiscoData2API.Misc
{
    public static class SqlHelper
    {
        public static bool IsSafeSql(string sql)
        {
            var blacklist = new[] { ";", "--", "/*", "*/", "xp_", "sp_", "EXEC", "DROP", "INSERT", "DELETE", "ALTER", "CREATE" };
            return !blacklist.Any(keyword => sql.IndexOf(keyword, StringComparison.OrdinalIgnoreCase) >= 0);
        }

        private static readonly HashSet<string> _allowedAggregateFunctions = new(StringComparer.OrdinalIgnoreCase)
            { "COUNT", "SUM", "AVG", "MIN", "MAX", "DATE_TRUNC" };
        private static readonly HashSet<string> _allowedGranularities = new(StringComparer.OrdinalIgnoreCase)
            { "day", "week", "month", "quarter", "year" };

        public static string BuildQuery(string dataset, int defaultLimit, string[]? fields = null, int? limit = null, int? offset = null, FilterDefinition[]? filters = null, string[]? groupBy = null, AggregateDefinition[]? aggregates = null)
        {
            var quotedTable = string.Join(".", dataset.Split('.').Select(p => $"\"{p}\""));
            var selectedFields = fields != null && fields.Length > 0
                ? string.Join(", ", fields.Select(f => $"\"{f}\""))
                : "*";
            limit = limit.HasValue && limit != 0 ? limit.Value : defaultLimit;

            if (aggregates != null && aggregates.Length > 0)
            {
                var aggExpressions = aggregates.Select(a =>
                {
                    if (!_allowedAggregateFunctions.Contains(a.Function))
                        throw new SQLFormattingException($"Aggregate function '{a.Function}' is not allowed.");

                    var func = a.Function.ToUpperInvariant();
                    var field = a.Field == "*" ? "*" : $"\"{a.Field}\"";
                    string expr;

                    if (func == "DATE_TRUNC")
                    {
                        if (string.IsNullOrWhiteSpace(a.Granularity) || !_allowedGranularities.Contains(a.Granularity))
                            throw new SQLFormattingException($"DATE_TRUNC requires a valid granularity (day, week, month, quarter, year).");
                        expr = $"DATE_TRUNC('{a.Granularity.ToLowerInvariant()}', {field})";
                    }
                    else
                    {
                        expr = $"{func}({field})";
                    }

                    return string.IsNullOrWhiteSpace(a.Alias) ? expr : $"{expr} AS \"{a.Alias}\"";
                });

                var fieldsPart = selectedFields == "*" ? "" : selectedFields + ", ";
                selectedFields = fieldsPart + string.Join(", ", aggExpressions);
            }

            var query = $"SELECT {selectedFields} FROM {quotedTable}";

            if (filters != null && filters.Length > 0)
                query += $" WHERE 1=1 {string.Join(" ", filters.Select(BuildFilterClause))}";

            if (groupBy != null && groupBy.Length > 0)
                query += $" GROUP BY {string.Join(", ", groupBy.Select(g => $"\"{g}\""))}";

            query += $" LIMIT {limit}";

            if (offset.HasValue && offset > 0)
                query += $" OFFSET {offset}";

            if (!SQLExtensions.ValidateSQL(query))
                throw new SQLFormattingException("SQL query contains unsafe keywords.");

            return query;
        }

        public static string BuildFilterClause(FilterDefinition filter)
        {
            var values = filter.Values?.Select(QuoteValue).ToArray() ?? [];

            return filter.Condition.ToUpper().Trim() switch
            {
                "BETWEEN" => $" {filter.Concat} (\"{filter.FieldName}\" BETWEEN {string.Join(" AND ", values)})",
                "IN"      => $" {filter.Concat} (\"{filter.FieldName}\" IN ({string.Join(", ", values)}))",
                _         => $" {filter.Concat} (\"{filter.FieldName}\" {filter.Condition} {string.Join(", ", values)})"
            };
        }

        public static string QuoteValue(string value)
        {
            if (value.Equals("NULL", StringComparison.OrdinalIgnoreCase))
                return "NULL";
            if (value.StartsWith('\'') && value.EndsWith('\'') && value.Length >= 2)
                return value;
            if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
                return value;
            return $"'{value.Replace("'", "''")}'";
        }
    }
}
