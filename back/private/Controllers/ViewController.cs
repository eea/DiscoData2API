
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
    public class ViewController(ILogger<ViewController> logger, MongoService mongoService, DremioService dremioService) : ControllerBase
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
                    ID = System.Guid.NewGuid().ToString(),
                    Query = request.Query,
                    Name = request.Name,
                    UserAdded = request.UserAdded,
                    Version = request.Version,
                    Description = request.Description,
                    Fields = ExtractFieldsFromQuery(request.Query).Result,
                    IsActive = true,
                    Date = DateTime.Now
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

                if (!hasId && string.IsNullOrEmpty(doc.ID)) doc.ID = System.Guid.NewGuid().ToString();
                //we update the fields in case the query changed
                doc.Query = request.Query;
                doc.Name = request.Name;
                if (!string.IsNullOrEmpty(request.UserAdded)) doc.UserAdded = request.UserAdded;
                if (!string.IsNullOrEmpty(request.Version)) doc.Version = request.Version;
                doc.Description = request.Description;
                doc.Fields = await ExtractFieldsFromQuery(request.Query);
                doc.IsActive = true;
                doc.Date = DateTime.Now;

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
        /// <param name="userAdded">The username that creatde the query</param>
        /// <returns>Return a list of MongoDocument class</returns>
        /// <response code="200">Returns the catalogue</response>        
        /// <response code="408">If the request times out</response>
        [HttpGet("GetCatalog")]
        public async Task<ActionResult<List<MongoDocument>>> GetMongoCatalog([FromQuery] string? userAdded)
        {
            try
            {
                if (string.IsNullOrEmpty(userAdded))
                {
                    return await mongoService.GetAllAsync();
                }

                return await mongoService.GetAllByUserAsync(userAdded);
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
        [HttpPost("{id}")]
        public async Task<ActionResult<string>> ExecuteQuery(string id, [FromBody] QueryRequest request)
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
                else
                {
                    mongoDoc.Query = UpdateQueryString(mongoDoc.Query, request.Fields, request.Limit, request.Filters);
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
                //show the first line of the error.
                //Rest of the lines show the query


                logger.LogError(message: ex.Message);
                string errmsg = ((Grpc.Core.RpcException)ex).Status.Detail;
                /*
                if (errmsg != null)
#pragma warning disable CS8600 // Se va a convertir un literal nulo o un posible valor nulo en un tipo que no acepta valores NULL
                    errmsg = errmsg.Split(['\r', '\n'])
                        .FirstOrDefault();
#pragma warning restore CS8600 // Se va a convertir un literal nulo o un posible valor nulo en un tipo que no acepta valores NULL
                */
                return StatusCode(StatusCodes.Status400BadRequest, errmsg);
            }
        }

        #region helper

        private string UpdateQueryString(string query, string[]? fields, int? limit, FilterDefinition[]? filters)
        {
            // Update fields returned by query
            fields = fields != null && fields.Length > 0 ? fields : ["*"];
            var _query_aux = query;
            _query_aux = _query_aux.Replace("*", string.Join(",", fields));  //used for debugging

            // Add filters to query if they exist
            string filter_query = string.Empty;
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
                logger.LogWarning("SQL query contains unsafe keywords.");
                throw new SQLFormattingException("SQL query contains unsafe keywords.");
            }
            return full_query;
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