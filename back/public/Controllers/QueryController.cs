using DiscoData2API.Services;
using DiscoData2API.Class;
using DiscoData2API.Model;
using DiscoData2API.Misc;
using Microsoft.AspNetCore.Mvc;
using System.Text;
using Microsoft.AspNetCore.DataProtection.AuthenticatedEncryption.ConfigurationModel;


namespace DiscoData2API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class QueryController : ControllerBase
    {
        private readonly ILogger<QueryController> _logger;
        private readonly MongoService _mongoService;
        private readonly DremioService _dremioService;
        private readonly int _defaultLimit;
        private readonly int _timeout;

        public QueryController(ILogger<QueryController> logger, MongoService mongoService, DremioService dremioService)
        {
            _logger = logger;
            _mongoService = mongoService;
            _dremioService = dremioService;
            _defaultLimit = dremioService._limit;
            _timeout = dremioService._timeout;
        }

        /// <summary>
        /// Get catalog of pre-processed views
        /// </summary>
        /// <returns>Returns a list of pre-processed views</returns>
        /// 
        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoPublicDocument>>> GetMongoCatalog()
        {
            return await  _mongoService.GetAllAsync();
        }


        /// <summary>
        /// Get a view by ID
        /// </summary>
        /// <returns>Returns a view</returns>
        /// 
        [HttpGet("Get/{id}")]
        public async Task<ActionResult<MongoPublicDocument>> GetById(string id)
        {
            return await _mongoService.GetById(id);
        }


        /// <summary>
        /// Executes a query and returns a JSON with the results
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
        ///     "filters": ["column1 = 'value1'", "column2 = 'value2'"],
        ///     "limit": 100,
        ///     }
        ///         
        /// </remarks>
        /// <response code="201">Returns the newly created item</response>
        /// <response code="400">If the item is null</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("{id}")]
        public async Task<ActionResult<string>> ExecuteQuery(string id, [FromBody] QueryRequest request)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_timeout)); // Creates a CancellationTokenSource with a 5-second timeout
            try
            {
                MongoDocument? mongoDoc = await _mongoService.GetFullDocumentById(id);

                if (mongoDoc == null)
                {
                    _logger.LogError($"Query with id {id} not found");
                    return NotFound();
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
                filter_query = string.Join(" ", filters.Select(a=> a.BuildFilterString())) ;
                foreach (var filter in filters)
                {
                    _filter_query.AppendFormat(" {0} ", filter.BuildFilterString() );
                }
            }
            

            // Ensure LIMIT is always at the end
            limit = limit.HasValue && limit != 0 ? limit.Value : _defaultLimit;

            // Remove any existing LIMIT clause and append a new one
            query = System.Text.RegularExpressions.Regex.Replace(query, @"LIMIT\s+\d+", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase);

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