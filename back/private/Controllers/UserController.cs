using DiscoData2API_Priv.Services;
using DiscoData2API_Priv.Model;
using Microsoft.AspNetCore.Mvc;
using DiscoData2API_Priv.Misc;

namespace DiscoData2API_Priv.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UserController(ILogger<UserController> logger, MongoService mongoService) : ControllerBase
    {
        /// <summary>
        /// Create a new user
        /// </summary>
        /// <param name="request">The JSON body of the request</param>
        /// <returns>Returns the newly created user</returns>
        /// <response code="200">Returns the newly created user</response>
        /// <response code="400">If the request fires an error or user already exists</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("CreateUser")]
        public async Task<ActionResult<MongoUser>> CreateUser([FromBody] MongoUserBaseDocument request)
        {
            try
            {
                var newUser = new MongoUser()
                {
                    UserName = request.UserName,
                    UserAccess = request.UserAccess ?? new List<UserProjectAccess>(),
                    IsActive = true,
                    CreatedAt = DateTime.Now,
                    LastUpdated = DateTime.Now
                };

                var createdUser = await mongoService.CreateUserAsync(newUser);
                if (createdUser == null)
                {
                    return BadRequest($"User with username '{request.UserName}' already exists.");
                }

                return Ok(createdUser);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
        }

        /// <summary>
        /// Get a user by ID
        /// </summary>
        /// <param name="id">The user ID</param>
        /// <returns>Returns a user</returns>
        /// <response code="200">Returns user</response>
        /// <response code="404">If the user does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("Get/{id}")]
        public async Task<ActionResult<MongoUser>> GetById(string id)
        {
            try
            {
                return await mongoService.ReadUserAsync(id);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                logger.LogError(string.Format("Cannot retrieve user with id {0}", id));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find user {id}");
            }
        }

        /// <summary>
        /// Get a user by username
        /// </summary>
        /// <param name="username">The username</param>
        /// <returns>Returns a user</returns>
        /// <response code="200">Returns user</response>
        /// <response code="404">If the user does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("GetByUsername/{username}")]
        public async Task<ActionResult<MongoUser>> GetByUsername(string username)
        {
            try
            {
                return await mongoService.ReadUserByUsernameAsync(username);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                logger.LogError(string.Format("Cannot retrieve user with username {0}", username));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find user {username}");
            }
        }

        /// <summary>
        /// Update a user
        /// </summary>
        /// <param name="id">The MongoDB ObjectId of the user to update</param>
        /// <param name="request">The JSON body of the request</param>
        /// <returns>Returns the updated MongoUser</returns>
        /// <response code="200">Returns the newly updated user</response>
        /// <response code="400">If the request fires an error</response>
        /// <response code="404">If the user does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("UpdateUser/{id}")]
        public async Task<ActionResult<MongoUser>> UpdateUser(string id, [FromBody] MongoUserBaseDocument request)
        {
            try
            {
                MongoUser? user = await mongoService.ReadUserAsync(id, false);

                if (user == null) throw new ViewNotFoundException();

                user.UserName = request.UserName;
                if (request.UserAccess != null) user.UserAccess = request.UserAccess;

                var updatedUser = await mongoService.UpdateUserAsync(id, user);
                if (updatedUser == null)
                {
                    logger.LogWarning($"User with id {id} could not be updated.");
                    return NotFound($"User with id {id} not found.");
                }

                return Ok(updatedUser);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find user {id}");
            }
            catch (Exception ex)
            {
                logger.LogError(message: ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
        }

        /// <summary>
        /// Delete a User
        /// </summary>
        /// <param name="id">The user ID</param>
        /// <returns>Return True if deleted</returns>
        /// <response code="404">If the user does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpDelete("DeleteUser/{id}")]
        public async Task<ActionResult<bool>> DeleteUser(string id)
        {
            try
            {
                var user = await mongoService.ReadUserAsync(id);
                return await mongoService.DeleteUserAsync(id);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                logger.LogError(string.Format("Cannot retrieve user with id {0}", id));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find user {id}");
            }
        }

        /// <summary>
        /// Get all users with their project access
        /// </summary>
        /// <returns>Return a list of MongoUser class</returns>
        /// <response code="200">Returns the users catalogue</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("GetAllUsers")]
        public async Task<ActionResult<List<MongoUser>>> GetAllUsers()
        {
            try
            {
                return await mongoService.GetAllUsersAsync();
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
        }

        /// <summary>
        /// Get all projects that a user has access to
        /// </summary>
        /// <param name="userId">The user ID</param>
        /// <returns>Return a list of MongoProject class</returns>
        /// <response code="200">Returns the user's projects</response>
        /// <response code="404">If the user does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("GetUserProjects/{userId}")]
        public async Task<ActionResult<List<MongoProject>>> GetUserProjects(string userId)
        {
            try
            {
                var user = await mongoService.ReadUserAsync(userId, false);
                if (user == null)
                {
                    return StatusCode(StatusCodes.Status404NotFound, $"Cannot find user {userId}");
                }

                return await mongoService.GetUserProjectsAsync(userId);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
        }

        /// <summary>
        /// Give a user access to a project
        /// </summary>
        /// <param name="request">The user and project access request</param>
        /// <returns>Return True if access was granted</returns>
        /// <response code="200">Returns success status</response>
        /// <response code="400">If the request is invalid</response>
        /// <response code="404">If the user or project does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("AddProjectAccess")]
        public async Task<ActionResult<bool>> AddProjectAccess([FromBody] UserProjectAccessRequest request)
        {
            try
            {
                var user = await mongoService.ReadUserAsync(request.UserId, false);
                if (user == null)
                {
                    return StatusCode(StatusCodes.Status404NotFound, $"Cannot find user {request.UserId}");
                }

                var project = await mongoService.ReadProjectAsync(request.ProjectId, false);
                if (project == null)
                {
                    return StatusCode(StatusCodes.Status404NotFound, $"Cannot find project {request.ProjectId}");
                }

                var result = await mongoService.AddProjectAccessAsync(request.UserId, request.ProjectId);
                return Ok(result);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
        }

        /// <summary>
        /// Remove a user's access to a project
        /// </summary>
        /// <param name="request">The user and project access request</param>
        /// <returns>Return True if access was removed</returns>
        /// <response code="200">Returns success status</response>
        /// <response code="400">If the request is invalid</response>
        /// <response code="404">If the user does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("RemoveProjectAccess")]
        public async Task<ActionResult<bool>> RemoveProjectAccess([FromBody] UserProjectAccessRequest request)
        {
            try
            {
                var user = await mongoService.ReadUserAsync(request.UserId, false);
                if (user == null)
                {
                    return StatusCode(StatusCodes.Status404NotFound, $"Cannot find user {request.UserId}");
                }

                var result = await mongoService.RemoveProjectAccessAsync(request.UserId, request.ProjectId);
                return Ok(result);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
        }
    }
}