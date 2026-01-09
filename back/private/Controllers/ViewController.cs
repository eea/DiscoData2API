
using DiscoData2API_Priv.Services;
using DiscoData2API_Priv.Model;
using Microsoft.AspNetCore.Mvc;
using DiscoData2API_Priv.Misc;
using System.Text;
using DiscoData2API.Class;
using static System.Runtime.InteropServices.JavaScript.JSType;
using System.Xml.Linq;

namespace DiscoData2API_Priv.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ViewController(ILogger<ViewController> logger, MongoService mongoService, DremioService dremioService, ParameterSubstitutionService parameterService, QueryThrottlingService throttlingService) : ControllerBase
    {
        private readonly int _defaultLimit = dremioService._limit;
        private readonly int _timeout = dremioService._timeout;

        /// <summary>
        /// Create a view (MongoDB)
        /// </summary>
        /// <param name="request">The JSON body of the request</param>
        /// <returns>Returns the Json document saved</returns>
        /// <response code="200">Returns the newly created query</response>
        /// <response code="400">If the query fires an error in the execution</response>        
        /// <response code="408">If the request times out</response>
        [HttpPost("CreateView")]
        public async Task<ActionResult<MongoDocument>> CreateView([FromBody] MongoBaseDocument request)
        {
            try
            {
                if (string.IsNullOrEmpty(request.Query) || !SQLExtensions.ValidateSQL(request.Query))
                {
                    logger.LogWarning("SQL query contains unsafe keywords.");
                    return BadRequest("SQL query contains unsafe keywords.");
                }

                return await mongoService.CreateAsync(new MongoDocument()
                {
                    Query = request.Query,
                    Name = request.Name,
                    UserAdded = request.UserAdded,
                    Version = request.Version,
                    Description = request.Description,
                    Fields = ExtractFieldsFromQuery(request.Query).Result,
                    Parameters = request.Parameters,
                    IsActive = true,
                    Date = DateTime.Now,
                    Catalog = request.Catalog,
                    ProjectId = request.ProjectId
                });
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (SQLFormattingException ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);

            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
        }

        /// <summary>
        /// Get a view by ID
        /// </summary>
        /// <param name="id">The query ID</param>
        /// <returns>Returns a view</returns>
        /// <response code="200">Returns view</response>
        /// <response code="404">If the view does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("Get/{id}")]
        public async Task<ActionResult<MongoDocument>> GetById(string id)
        {
            try
            {
                return await mongoService.ReadAsync(id);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                logger.LogError(string.Format("Cannot retrieve view with id {0}", id));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");

            }
        }

        /// <summary>
        /// Update a view (MongoDB)
        /// </summary>
        /// <param name="id">The Id (UUID or MongoDB Id) of the view to update. If the UUID does not exist it will search for MongoDB ID </param>
        /// <param name="request">The JSON body of the request</param>
        /// <returns>Returns the updated MongoDocument</returns>
        /// <response code="200">Returns the newly updated query</response>
        /// <response code="400">If the query fires an error in the execution</response>
        /// <response code="404">If the view does not exist</response>
        /// <response code="408">If the request times out</response>    
        [HttpPost("UpdateView/{id}")]
        public async Task<ActionResult<MongoDocument>> UpdateView(string id, [FromBody] MongoBaseDocument request)
        {
            try
            {
                bool hasId = true;
                MongoDocument? doc = await mongoService.ReadAsync(id, false);
                //if the ID was not created. Try finding by MongoDB id
                if (doc == null)
                {
                    doc = await mongoService.ReadByMongoIDAsync(id);
                    hasId = false;
                }

                if (doc == null) throw new ViewNotFoundException();


                if (string.IsNullOrEmpty(request.Query) || !SQLExtensions.ValidateSQL(request.Query))
                {
                    logger.LogWarning("SQL query contains unsafe keywords.");
                    return BadRequest("SQL query contains unsafe keywords.");
                }

                //we update the fields in case the query changed
                doc.Query = request.Query;
                doc.Name = request.Name;
                if (!string.IsNullOrEmpty(request.UserAdded)) doc.UserAdded = request.UserAdded;
                if (!string.IsNullOrEmpty(request.Version)) doc.Version = request.Version;
                doc.Description = request.Description;
                doc.Fields = await ExtractFieldsFromQuery(request.Query);
                doc.Parameters = request.Parameters;
                doc.IsActive = true;
                doc.Date = DateTime.Now;
                if (!string.IsNullOrEmpty(request.Catalog)) doc.Catalog = request.Catalog;
                if (!string.IsNullOrEmpty(request.ProjectId)) doc.ProjectId = request.ProjectId;

                var updatedDocument = await mongoService.UpdateAsync(id, doc, hasId);
                if (updatedDocument == null)
                {
                    logger.LogWarning($"Document with id {id} could not be updated.");
                    return NotFound($"Document with id {id} not found.");
                }

                return Ok(updatedDocument);
            }

            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");
            }

            catch (SQLFormattingException ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);

            }
            catch (Exception ex)
            {
                //show the first line of the error.
                //Rest of the lines show the query

                logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;

                return StatusCode(StatusCodes.Status400BadRequest, errmsg);
            }
        }

        /// <summary>
        /// Delete a View (MongoDB)
        /// </summary>
        /// <param name="id"></param>
        /// <returns>Return True if deleted</returns>
        /// <response code="404">If the view does not exist</response>
        /// <response code="408">If the request times out</response>

        [HttpDelete("DeleteView/{id}")]
        public async Task<ActionResult<bool>> DeleteView(string id)
        {
            try
            {
                var doc = await mongoService.ReadAsync(id);
                return await mongoService.DeleteAsync(id);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                logger.LogError(string.Format("Cannot retrieve view with id {0}", id));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");

            }
        }

        /// <summary>
        /// Get catalog of queries (MongoDB)
        /// </summary>
        /// <param name="userAdded">The username that created the query</param>
        /// <param name="projectId">The project ID to filter by</param>
        /// <returns>Return a list of MongoDocument class</returns>
        /// <response code="200">Returns the catalogue</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoDocument>>> GetMongoCatalog([FromQuery] string? userAdded, [FromQuery] string? projectId)
        {
            try
            {
                // Handle different filtering combinations
                if (!string.IsNullOrEmpty(projectId))
                {
                    return await mongoService.GetAllByProjectAsync(projectId, userAdded);
                }
                else if (!string.IsNullOrEmpty(userAdded))
                {
                    return await mongoService.GetAllByUserAsync(userAdded);
                }
                else
                {
                    return await mongoService.GetAllAsync();
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
        }


        /// <summary>
        /// Executes a query and returns a JSON with the results
        /// </summary>
        /// <param name="id">The query ID</param>
        /// <returns></returns>
        /// <remarks>
        /// Sample request:
        ///   GET /api/query/672b84ef75e2d0b792658f24
        /// </remarks>
        /// <response code="200">Returns the newly created item</response>
        /// <response code="400">If the query fires an error in the execution</response>
        /// <response code="404">If the view does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("{id}")]
        public async Task<ActionResult<string>> ExecuteQuery(string id)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                MongoDocument? mongoDoc = await mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    logger.LogError($"Query with id {id} not found");
                    throw new ViewNotFoundException();
                }
                var result = await dremioService.ExecuteQuery(mongoDoc.Query, cts.Token);

                return result;
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");
            }
            catch (SQLFormattingException ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);

            }
            catch (Exception ex)
            {
                logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;
                return StatusCode(StatusCodes.Status400BadRequest, errmsg);
            }
        }

        /// <summary>
        /// Executes a query with optional parameters and filters, returning complete JSON results
        /// </summary>
        /// <param name="id">The query ID</param>
        /// <param name="request">The JSON body containing optional parameters, fields, filters, and limit</param>
        /// <returns></returns>
        /// <remarks>
        /// Unified endpoint that handles both parameterized queries and filtering. All fields are optional.
        ///
        /// Sample request:
        ///
        ///     POST /api/ViewController/Execute/672b84ef75e2d0b792658f24
        ///     {
        ///         "parameters": {
        ///             "country_code": "ES",
        ///             "year": "2023"
        ///         },
        ///         "fields": ["column1", "column2"],
        ///         "filters": [
        ///             {"Concat": "AND", "FieldName":"Column4", "Condition":"=", "Values": ["'Value1'"] }
        ///         ],
        ///         "limit": 100
        ///     }
        ///
        /// For simple filtering without parameters:
        ///     {
        ///         "fields": ["column1", "column2"],
        ///         "filters": [...],
        ///         "limit": 100
        ///     }
        ///
        /// </remarks>
        /// <response code="200">Returns the query results</response>
        /// <response code="400">If the query fires an error or parameters are invalid</response>
        /// <response code="404">If the view does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("Execute/{id}")]
        public async Task<ActionResult<string>> ExecuteQuery(string id, [FromBody] QueryExecutionRequest? request)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
            try
            {
                MongoDocument? mongoDoc = await mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    logger.LogError($"Query with id {id} not found");
                    throw new ViewNotFoundException();
                }

                var processedQuery = mongoDoc.Query;

                // Substitute parameters if there are any parameters defined (uses defaults if no values provided)
                if (mongoDoc.Parameters != null && mongoDoc.Parameters.Count > 0)
                {
                    processedQuery = parameterService.SubstituteParameters(
                        processedQuery,
                        mongoDoc.Parameters,
                        request?.Parameters
                    );
                }

                // Apply additional filters if provided
                if ((request?.Fields != null && request.Fields.Length > 0) ||
                    (request?.Filters != null && request.Filters.Length > 0) ||
                    request?.Limit != null)
                {
                    processedQuery = UpdateQueryString(processedQuery, request.Fields, request.Limit, request.Filters);
                }

                var result = await dremioService.ExecuteQuery(processedQuery, cts.Token);
                return result;
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");
            }
            catch (ArgumentException ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
            catch (SQLFormattingException ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
            catch (Exception ex)
            {
                logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;
                return StatusCode(StatusCodes.Status400BadRequest, errmsg);
            }
        }

        /// <summary>
        /// Executes a query with optional parameters and filters, streaming results for large datasets
        /// </summary>
        /// <param name="id">The query ID</param>
        /// <param name="request">The JSON body containing optional parameters, fields, filters, and limit</param>
        /// <returns>Streaming JSON response</returns>
        /// <remarks>
        /// Unified streaming endpoint that handles both parameterized queries and filtering. All fields are optional.
        /// Use this endpoint for large result sets to avoid memory issues and timeouts.
        ///
        /// Sample request:
        ///
        ///     POST /api/ViewController/Stream/672b84ef75e2d0b792658f24
        ///     {
        ///         "parameters": {
        ///             "country_code": "ES",
        ///             "year": "2023"
        ///         },
        ///         "fields": ["column1", "column2"],
        ///         "filters": [
        ///             {"Concat": "AND", "FieldName":"Column4", "Condition":"=", "Values": ["'Value1'"] }
        ///         ],
        ///         "limit": 100000
        ///     }
        ///
        /// For simple filtering without parameters:
        ///     {
        ///         "fields": ["column1", "column2"],
        ///         "filters": [...],
        ///         "limit": 100000
        ///     }
        ///
        /// </remarks>
        /// <response code="200">Returns streaming query results</response>
        /// <response code="400">If the query fires an error or parameters are invalid</response>
        /// <response code="404">If the view does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("Stream/{id}")]
        public async Task StreamQuery(string id, [FromBody] QueryExecutionRequest? request)
        {
            var queryId = $"{id}-{Guid.NewGuid():N}";
            var maxRows = request?.Limit ?? _defaultLimit;
            var dynamicTimeout = dremioService.GetTimeoutForQuery(maxRows);

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(dynamicTimeout));
            using var throttleToken = await throttlingService.AcquireQuerySlotAsync(queryId, cts.Token);

            try
            {
                MongoDocument? mongoDoc = await mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    logger.LogError($"Query with id {id} not found");
                    Response.StatusCode = 404;
                    await Response.WriteAsync($"Cannot find view {id}");
                    return;
                }

                var processedQuery = mongoDoc.Query;

                // Substitute parameters if there are any parameters defined (uses defaults if no values provided)
                if (mongoDoc.Parameters != null && mongoDoc.Parameters.Count > 0)
                {
                    processedQuery = parameterService.SubstituteParameters(
                        processedQuery,
                        mongoDoc.Parameters,
                        request?.Parameters
                    );
                }

                // Apply additional filters if provided
                if ((request?.Fields != null && request.Fields.Length > 0) ||
                    (request?.Filters != null && request.Filters.Length > 0) ||
                    request?.Limit != null)
                {
                    processedQuery = UpdateQueryString(processedQuery, request.Fields, request.Limit, request.Filters);
                }

                // Set response headers for streaming
                Response.ContentType = "application/json";
                Response.Headers["Cache-Control"] = "no-cache";
                Response.Headers["X-Content-Type-Options"] = "nosniff";
                Response.Headers["X-Query-Id"] = queryId;
                Response.Headers["X-Max-Rows"] = maxRows.ToString();

                // Stream the results directly to the response
                var rowCount = await dremioService.ExecuteQueryStream(processedQuery, Response.Body, cts.Token, maxRows);

                logger.LogInformation($"Streamed {rowCount} rows for view {id} (query: {queryId})");
            }
            catch (OperationCanceledException)
            {
                logger.LogError($"Query {queryId} was canceled due to timeout.");
                Response.StatusCode = 408;
                await Response.WriteAsync("Request timed out.");
            }
            catch (ArgumentException ex)
            {
                logger.LogError(ex, $"Argument error in query {queryId}");
                Response.StatusCode = 400;
                await Response.WriteAsync(ex.Message);
            }
            catch (SQLFormattingException ex)
            {
                logger.LogError(ex, $"SQL formatting error in query {queryId}");
                Response.StatusCode = 400;
                await Response.WriteAsync(ex.Message);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Error executing query {queryId}");
                Response.StatusCode = 400;
                var errorMsg = ex is Grpc.Core.RpcException rpcEx ? rpcEx.Status.Detail : ex.Message;
                await Response.WriteAsync(errorMsg);
            }
        }
        

        #region helper

        private string UpdateQueryString(string query, string[]? fields, int? limit, FilterDefinition[]? filters)
        {
            // Update fields returned by query
            // Filter out invalid field names like "string", "additionalProp1", etc. (common from Swagger examples)
            var invalidFieldNames = new[] { "string", "additionalProp1", "additionalProp2", "additionalProp3" };

            // Handle null, empty array, or arrays with only invalid field names
            if (fields == null || fields.Length == 0)
            {
                // Null or empty array - use all fields
                fields = ["*"];
            }
            else
            {
                // Remove invalid/example field names
                fields = fields.Where(f => !invalidFieldNames.Contains(f, StringComparer.OrdinalIgnoreCase)).ToArray();
                // If no valid fields remain after filtering, use "*"
                if (fields.Length == 0)
                {
                    fields = ["*"];
                }
            }

            // Remove any existing LIMIT clause from the original query to avoid conflicts
            var cleanedQuery = RemoveLimitClause(query);

            // Add filters to query if they exist
            string filter_query = string.Empty;
            StringBuilder _filter_query = new();
            if (filters != null && filters.Length > 0)
            {
                // Filter out invalid filters (empty fieldName or condition)
                var validFilters = filters.Where(f => !string.IsNullOrWhiteSpace(f.FieldName) && !string.IsNullOrWhiteSpace(f.Condition)).ToArray();

                if (validFilters.Length > 0)
                {
                    filter_query = string.Join(" ", validFilters.Select(a => a.BuildFilterString()));
                    foreach (var filter in validFilters)
                    {
                        _filter_query.AppendFormat(" {0} ", filter.BuildFilterString());
                    }
                }
            }

            // Ensure LIMIT is always at the end
            var finalLimit = limit.HasValue && limit != 0 ? limit.Value : _defaultLimit;

            string full_query;
            if (string.IsNullOrEmpty(filter_query))
            {
                // No filters - add LIMIT to original query
                full_query = string.Format("{0} LIMIT {1}", cleanedQuery, finalLimit);
            }
            else
            {
                // Insert filters in the correct position (after WHERE, before ORDER BY/GROUP BY)
                full_query = InsertFiltersIntoQuery(cleanedQuery, filter_query.ToString(), finalLimit);
            }

            // Handle field selection if not all fields (*)
            if (fields.Length == 1 && fields[0] == "*")
            {
                // Keep the query as is - selecting all fields
            }
            else
            {
                // Need to wrap in select for specific field selection
                full_query = string.Format("select {0} from ({1})", string.Join(",", fields), full_query);
            }

            if (!SQLExtensions.ValidateSQL(full_query))
            {
                logger.LogWarning("SQL query contains unsafe keywords.");
                throw new SQLFormattingException("SQL query contains unsafe keywords.");
            }
            return full_query;
        }

        private string RemoveLimitClause(string query)
        {
            // Use regex to remove LIMIT clause (case insensitive)
            // This pattern matches LIMIT followed by optional whitespace, then digits, at the end of the query
            var limitPattern = @"\s+LIMIT\s+\d+\s*$";
            var cleanedQuery = System.Text.RegularExpressions.Regex.Replace(query.Trim(), limitPattern, "", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return cleanedQuery;
        }

        private bool HasWhereClause(string query)
        {
            // Use regex to check if query contains a WHERE clause
            // This looks for WHERE keyword not within quotes or subqueries (simplified check)
            var wherePattern = @"\bWHERE\b";
            return System.Text.RegularExpressions.Regex.IsMatch(query, wherePattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        }

        private string InsertFiltersIntoQuery(string query, string filterQuery, int limit)
        {
            // Find positions of ORDER BY, GROUP BY, HAVING clauses to insert filters before them
            var orderByPattern = @"\bORDER\s+BY\b";
            var groupByPattern = @"\bGROUP\s+BY\b";
            var havingPattern = @"\bHAVING\b";

            var orderByMatch = System.Text.RegularExpressions.Regex.Match(query, orderByPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            var groupByMatch = System.Text.RegularExpressions.Regex.Match(query, groupByPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            var havingMatch = System.Text.RegularExpressions.Regex.Match(query, havingPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);

            // Find the earliest position where we need to insert filters
            int insertPosition = query.Length;
            if (orderByMatch.Success) insertPosition = Math.Min(insertPosition, orderByMatch.Index);
            if (groupByMatch.Success) insertPosition = Math.Min(insertPosition, groupByMatch.Index);
            if (havingMatch.Success) insertPosition = Math.Min(insertPosition, havingMatch.Index);

            string beforeClauses = query.Substring(0, insertPosition).TrimEnd();
            string afterClauses = insertPosition < query.Length ? query.Substring(insertPosition) : "";

            string result;
            if (HasWhereClause(beforeClauses))
            {
                // Query already has WHERE - append filters with AND
                result = string.Format("{0} {1} {2} LIMIT {3}", beforeClauses, filterQuery, afterClauses, limit);
            }
            else
            {
                // No WHERE clause - add one with filters
                result = string.Format("{0} WHERE 1=1 {1} {2} LIMIT {3}", beforeClauses, filterQuery, afterClauses, limit);
            }

            return result.Trim();
        }

        #endregion

        #region Extract fields from query

        //use pure flight methods and properties to extract schema
        private async Task<List<Field>> ExtractFieldsFromQuery(string? query)
        {
            try
            {
                //var temp_table_name = string.Format("\"Local S3\".\"datahub-pre-01\".discodata.\"temp_{0}\"", System.Guid.NewGuid().ToString());
                using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
                var queryColumnsFlight = string.Format(@" select * from ({0} ) limit 1;", query);
                return await dremioService.GetSchema(queryColumnsFlight, cts.Token);
            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);

                throw; // new Exception("Invalid query");
            }
        }

        #endregion



    }
}