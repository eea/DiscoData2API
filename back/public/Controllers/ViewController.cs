using DiscoData2API.Services;
using DiscoData2API.Class;
using DiscoData2API.Model;
using DiscoData2API.Misc;
using Microsoft.AspNetCore.Mvc;
using System.Text;

namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ViewController(ILogger<ViewController> logger, MongoService mongoService, DremioService dremioService,
        ParameterSubstitutionService parameterService, QueryThrottlingService throttlingService) : ControllerBase
    {
        private readonly ILogger<ViewController> _logger = logger;
        private readonly MongoService _mongoService = mongoService;
        private readonly DremioService _dremioService = dremioService;
        private readonly ParameterSubstitutionService _parameterService = parameterService;
        private readonly QueryThrottlingService _throttlingService = throttlingService;
        private readonly int _defaultLimit = dremioService._limit;
        private readonly int _timeout = dremioService._timeout;

        /// <summary>
        /// Get catalog of pre-processed views
        /// </summary>
        /// <returns>Returns a list of pre-processed views</returns>
        /// <response code="200">Returns a list of active views</response>
        /// 
        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoPublicDocument>>> GetMongoCatalog()
        {
            return await _mongoService.GetAllAsync();
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:La plantilla debe ser una expresi�n est�tica", Justification = "<pendiente>")]
        public async Task<ActionResult<MongoPublicDocument>> GetById(string id)
        {
            try
            {
                return await _mongoService.GetById(id);
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                _logger.LogError(string.Format("Cannot retrieve view with id {0}", id));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");

            }
        }

        /// <summary>
        /// Executes a query with extra filters and returns a JSON with the results
        /// </summary>
        /// <param name="id">The query ID</param>
        /// <param name="request">The JSON body of the request</param>
        /// <returns></returns>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /api/query/672b84ef75e2d0b792658f24
        ///     {
        ///     "fields": ["column1", "column2"],
        ///     "filters": [
        ///         {"Concat": "AND", "FieldName":"Column4" ,"Condition":"=", "Values": ["'Value1'"]  } ,
        ///         {"Concat": "OR", "FieldName":"Column1" ,"Condition":"IN", "Values": ["'Value4'","'Value1'"]  }
        ///         ...
        ///     ],
        ///     "limit": 100,
        ///     }
        ///         
        /// </remarks>
        /// <response code="200">Returns the newly created item</response>
        /// <response code="400">If the query fires an error in the execution</response>
        /// <response code="404">If the view does not exist</response>
        /// <response code="408">If the request times out</response> 
        [HttpPost("Filtered/{id}")]
        public async Task<ActionResult<string>> ExecuteQueryFiltered(string id, [FromBody] QueryRequest request)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                MongoDocument? mongoDoc = await _mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    _logger.LogError($"Query with id {id} not found");
                    throw new ViewNotFoundException();
                }
                else
                {
                    mongoDoc.Query = UpdateQueryString(mongoDoc.Query, request.Fields, request.Limit, request.Filters);
                }
                var result = await _dremioService.ExecuteQuery(mongoDoc.Query, cts.Token);

                return result;
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");
            }

            catch (SQLFormattingException ex)
            {
                _logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);

            }

            catch (Exception ex)
            {
                //show the first line of the error.
                //Rest of the lines show the query


                _logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;
                if (errmsg != null)
#pragma warning disable CS8600 // Se va a convertir un literal nulo o un posible valor nulo en un tipo que no acepta valores NULL
                    errmsg = errmsg.Split(['\r', '\n'])
                        .FirstOrDefault();
#pragma warning restore CS8600 // Se va a convertir un literal nulo o un posible valor nulo en un tipo que no acepta valores NULL

                return StatusCode(StatusCodes.Status400BadRequest, errmsg);
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
                MongoDocument? mongoDoc = await _mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    _logger.LogError($"Query with id {id} not found");
                    throw new ViewNotFoundException();
                }
                var result = await _dremioService.ExecuteQuery(mongoDoc.Query, cts.Token);

                return result;
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");
            }
            catch (SQLFormattingException ex)
            {
                _logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);

            }
            catch (Exception ex)
            {
                _logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;
                if (errmsg != null)
#pragma warning disable CS8600 // Se va a convertir un literal nulo o un posible valor nulo en un tipo que no acepta valores NULL
                    errmsg = errmsg.Split(['\r', '\n'])
                        .FirstOrDefault();
#pragma warning restore CS8600 // Se va a convertir un literal nulo o un posible valor nulo en un tipo que no acepta valores NULL

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
        ///     POST /api/View/Execute/672b84ef75e2d0b792658f24
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
        public async Task<ActionResult<string>> ExecuteQuery(string id, [FromBody] QueryExecutionRequest request)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout));
            try
            {
                MongoDocument? mongoDoc = await _mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    _logger.LogError($"Query with id {id} not found");
                    throw new ViewNotFoundException();
                }

                var processedQuery = mongoDoc.Query;

                // Substitute parameters if provided
                if (request.Parameters != null && request.Parameters.Count > 0)
                {
                    processedQuery = _parameterService.SubstituteParameters(
                        processedQuery,
                        mongoDoc.Parameters,
                        request.Parameters
                    );
                }

                // Apply additional filters if provided
                if (request.Fields != null || request.Filters != null || request.Limit != null)
                {
                    processedQuery = UpdateQueryString(processedQuery, request.Fields, request.Limit, request.Filters);
                }

                var result = await _dremioService.ExecuteQuery(processedQuery, cts.Token);
                return result;
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find view {id}");
            }
            catch (SQLFormattingException ex)
            {
                _logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
            catch (ArgumentException ex)
            {
                _logger.LogError($"Parameter validation error: {ex.Message}");
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(message: ex.Message);
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
        ///     POST /api/View/Stream/672b84ef75e2d0b792658f24
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
        public async Task StreamQuery(string id, [FromBody] QueryExecutionRequest request)
        {
            var queryId = $"{id}-{Guid.NewGuid():N}";
            var maxRows = request.Limit ?? _defaultLimit;
            var dynamicTimeout = _dremioService.GetTimeoutForQuery(maxRows);

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(dynamicTimeout));
            using var throttleToken = await _throttlingService.AcquireQuerySlotAsync(queryId, cts.Token);

            try
            {
                MongoDocument? mongoDoc = await _mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    _logger.LogError($"Query with id {id} not found");
                    Response.StatusCode = 404;
                    await Response.WriteAsync($"Cannot find view {id}");
                    return;
                }

                var processedQuery = mongoDoc.Query;

                // Substitute parameters if provided
                if (request.Parameters != null && request.Parameters.Count > 0)
                {
                    processedQuery = _parameterService.SubstituteParameters(
                        processedQuery,
                        mongoDoc.Parameters,
                        request.Parameters
                    );
                }

                // Apply additional filters if provided
                if (request.Fields != null || request.Filters != null || request.Limit != null)
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
                var rowCount = await _dremioService.ExecuteQueryStream(processedQuery, Response.Body, cts.Token, maxRows);

                _logger.LogInformation($"Streamed {rowCount} rows for view {id} (query: {queryId})");
            }
            catch (OperationCanceledException)
            {
                _logger.LogError($"Query {queryId} was canceled due to timeout.");
                Response.StatusCode = 408;
                await Response.WriteAsync("Request timed out.");
            }
            catch (ArgumentException ex)
            {
                _logger.LogError($"Parameter validation error for query {queryId}: {ex.Message}");
                Response.StatusCode = 400;
                await Response.WriteAsync(ex.Message);
            }
            catch (SQLFormattingException ex)
            {
                _logger.LogError($"SQL formatting error for query {queryId}: {ex.Message}");
                Response.StatusCode = 400;
                await Response.WriteAsync(ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error executing query {queryId}");
                Response.StatusCode = 400;
                var errorMsg = ex is Grpc.Core.RpcException rpcEx ? rpcEx.Status.Detail : ex.Message;
                await Response.WriteAsync(errorMsg);
            }
        }

        #region helper

        private string UpdateQueryString(string query, string[]? fields, int? limit, FilterDefinition[]? filters)
        {
            // Update fields returned by query
            fields = fields != null && fields.Length > 0 ? fields : ["*"];
            var _query_aux = query;
            _query_aux = _query_aux.Replace("*", string.Join(",", fields));


            // Add filters to query if they exist
            string filter_query = String.Empty;
            StringBuilder _filter_query = new();
            if (filters != null && filters.Length > 0)
            {
                filter_query = string.Join(" ", filters.Select(a => a.BuildFilterString()));
                foreach (var filter in filters)
                {
                    _filter_query.AppendFormat(" {0} ", filter.BuildFilterString());
                }
            }

            // Ensure LIMIT is always at the end
            limit = limit.HasValue && limit != 0 ? limit.Value : _defaultLimit;

            string full_query;
            if (string.IsNullOrEmpty(filter_query))
                full_query = string.Format("select {0} from ({1}) ", string.Join(",", fields), query);
            else
                full_query = string.Format("select {0} from " +
                    "(select * from ({1}) WHERE 1=1 {2}) ", string.Join(",", fields), query, filter_query.ToString());

            full_query += $" LIMIT {limit}";

            if (!SQLExtensions.ValidateSQL(full_query))
            {
                _logger.LogWarning("SQL query contains unsafe keywords.");
                throw new SQLFormattingException("SQL query contains unsafe keywords.");
            }
            return full_query;
        }

        #endregion
    }
}