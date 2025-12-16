using DiscoData2API.Model;
using DiscoData2API.Misc;
using System.Text.RegularExpressions;

namespace DiscoData2API.Services
{
    public class ParameterSubstitutionService
    {
        private readonly ILogger<ParameterSubstitutionService> _logger;

        public ParameterSubstitutionService(ILogger<ParameterSubstitutionService> logger)
        {
            _logger = logger;
        }

        public string SubstituteParameters(string query, List<ViewParameter>? viewParameters, Dictionary<string, object>? providedParameters)
        {
            if (viewParameters == null || viewParameters.Count == 0)
                return query;

            var substitutedQuery = query;
            var parameterPattern = @"\{(\w+)\}";
            var matches = Regex.Matches(query, parameterPattern);

            foreach (Match match in matches)
            {
                var parameterName = match.Groups[1].Value;
                var viewParameter = viewParameters.FirstOrDefault(p => p.Name == parameterName);

                if (viewParameter == null)
                {
                    throw new ArgumentException($"Parameter '{parameterName}' is not defined for this view");
                }

                string? parameterValue = null;

                // Check if parameter value was provided
                if (providedParameters != null && providedParameters.ContainsKey(parameterName))
                {
                    parameterValue = providedParameters[parameterName]?.ToString();
                }
                // Use default value if available
                else if (!string.IsNullOrEmpty(viewParameter.DefaultValue))
                {
                    parameterValue = viewParameter.DefaultValue;
                }
                // Required parameter not provided
                else if (viewParameter.Required)
                {
                    throw new ArgumentException($"Required parameter '{parameterName}' was not provided");
                }

                if (parameterValue != null)
                {
                    // Validate parameter value
                    ValidateParameterValue(viewParameter, parameterValue);

                    // Sanitize parameter value based on type
                    var sanitizedValue = SanitizeParameterValue(viewParameter, parameterValue);

                    // Replace parameter in query
                    _logger.LogDebug($"Replacing parameter '{{{parameterName}}}' with value '{sanitizedValue}' in query");
                    substitutedQuery = substitutedQuery.Replace($"{{{parameterName}}}", sanitizedValue);
                    _logger.LogDebug($"Query after substitution: {substitutedQuery}");
                }
            }

            // Validate the final query for security
            if (!SQLExtensions.ValidateSQL(substitutedQuery))
            {
                throw new SQLFormattingException("Generated query contains unsafe keywords");
            }

            return substitutedQuery;
        }

        private void ValidateParameterValue(ViewParameter parameter, string value)
        {
            // Check allowed values if specified
            if (parameter.AllowedValues != null && parameter.AllowedValues.Count > 0)
            {
                if (!parameter.AllowedValues.Contains(value))
                {
                    throw new ArgumentException($"Parameter '{parameter.Name}' value '{value}' is not in allowed values: {string.Join(", ", parameter.AllowedValues)}");
                }
            }

            // Type-specific validation
            switch (parameter.Type.ToLower())
            {
                case "int":
                case "integer":
                    if (!int.TryParse(value, out _))
                    {
                        throw new ArgumentException($"Parameter '{parameter.Name}' must be an integer");
                    }
                    break;
                case "decimal":
                case "float":
                case "double":
                    if (!decimal.TryParse(value, out _))
                    {
                        throw new ArgumentException($"Parameter '{parameter.Name}' must be a decimal number");
                    }
                    break;
                case "date":
                    if (!DateTime.TryParse(value, out _))
                    {
                        throw new ArgumentException($"Parameter '{parameter.Name}' must be a valid date");
                    }
                    break;
            }
        }

        private string SanitizeParameterValue(ViewParameter parameter, string value)
        {
            _logger.LogDebug($"Sanitizing parameter '{parameter.Name}' of type '{parameter.Type}' with value '{value}'");

            switch (parameter.Type.ToLower())
            {
                case "string":
                    // Check if value is already quoted to prevent double-quoting
                    if (value.StartsWith("'") && value.EndsWith("'"))
                    {
                        _logger.LogDebug($"Value already quoted: '{value}'");
                        return value;
                    }
                    // Escape single quotes by doubling them and wrap in quotes for SQL
                    var sanitized = $"'{value.Replace("'", "''")}'";
                    _logger.LogDebug($"Sanitized value: '{sanitized}'");
                    return sanitized;
                case "int":
                case "integer":
                case "decimal":
                case "float":
                case "double":
                    // Numbers don't need quotes
                    return value;
                case "date":
                    // Format date and wrap in quotes
                    if (DateTime.TryParse(value, out DateTime date))
                    {
                        return $"'{date:yyyy-MM-dd}'";
                    }
                    return $"'{value}'";
                default:
                    // Default to string handling with double-quote protection
                    if (value.StartsWith("'") && value.EndsWith("'"))
                    {
                        _logger.LogDebug($"Default case - Value already quoted: '{value}'");
                        return value;
                    }
                    return $"'{value.Replace("'", "''")}'";
            }
        }

        public List<string> ExtractParametersFromQuery(string query)
        {
            var parameterPattern = @"\{(\w+)\}";
            var matches = Regex.Matches(query, parameterPattern);

            return matches
                .Cast<Match>()
                .Select(m => m.Groups[1].Value)
                .Distinct()
                .ToList();
        }
    }
}