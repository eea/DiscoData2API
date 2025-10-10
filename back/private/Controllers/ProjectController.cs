using DiscoData2API_Priv.Services;
using DiscoData2API_Priv.Model;
using Microsoft.AspNetCore.Mvc;
using DiscoData2API_Priv.Misc;

namespace DiscoData2API_Priv.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ProjectController(ILogger<ProjectController> logger, MongoService mongoService) : ControllerBase
    {
        /// <summary>
        /// Create a project (MongoDB)
        /// </summary>
        /// <param name="request">The JSON body of the request</param>
        /// <returns>Returns the Json document saved</returns>
        /// <response code="200">Returns the newly created project</response>
        /// <response code="400">If the request fires an error</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("CreateProject")]
        public async Task<ActionResult<MongoProject>> CreateProject([FromBody] MongoProjectBaseDocument request)
        {
            try
            {
                return await mongoService.CreateProjectAsync(new MongoProject()
                {
                    ProjectName = request.ProjectName,
                    ProjectDescription = request.ProjectDescription,
                    UserAdded = request.UserAdded,
                    IsActive = true,
                    CreationDate = DateTime.Now
                });
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
        /// Get a project by ID
        /// </summary>
        /// <param name="id">The project ID</param>
        /// <returns>Returns a project</returns>
        /// <response code="200">Returns project</response>
        /// <response code="404">If the project does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("Get/{id}")]
        public async Task<ActionResult<MongoProject>> GetById(string id)
        {
            try
            {
                return await mongoService.ReadProjectAsync(id);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                logger.LogError(string.Format("Cannot retrieve project with id {0}", id));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find project {id}");
            }
        }

        /// <summary>
        /// Update a project (MongoDB)
        /// </summary>
        /// <param name="id">The MongoDB ObjectId of the project to update</param>
        /// <param name="request">The JSON body of the request</param>
        /// <returns>Returns the updated MongoProject</returns>
        /// <response code="200">Returns the newly updated project</response>
        /// <response code="400">If the request fires an error</response>
        /// <response code="404">If the project does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpPost("UpdateProject/{id}")]
        public async Task<ActionResult<MongoProject>> UpdateProject(string id, [FromBody] MongoProjectBaseDocument request)
        {
            try
            {
                MongoProject? project = await mongoService.ReadProjectAsync(id, false);

                if (project == null) throw new ViewNotFoundException();

                project.ProjectName = request.ProjectName;
                project.ProjectDescription = request.ProjectDescription;
                if (!string.IsNullOrEmpty(request.UserAdded)) project.UserAdded = request.UserAdded;
                project.CreationDate = DateTime.Now;

                var updatedProject = await mongoService.UpdateProjectAsync(id, project);
                if (updatedProject == null)
                {
                    logger.LogWarning($"Project with id {id} could not be updated.");
                    return NotFound($"Project with id {id} not found.");
                }

                return Ok(updatedProject);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find project {id}");
            }
            catch (Exception ex)
            {
                logger.LogError(message: ex.Message);
                return StatusCode(StatusCodes.Status400BadRequest, ex.Message);
            }
        }

        /// <summary>
        /// Delete a Project (MongoDB)
        /// </summary>
        /// <param name="id">The project ID</param>
        /// <returns>Return True if deleted</returns>
        /// <response code="404">If the project does not exist</response>
        /// <response code="408">If the request times out</response>
        [HttpDelete("DeleteProject/{id}")]
        public async Task<ActionResult<bool>> DeleteProject(string id)
        {
            try
            {
                var project = await mongoService.ReadProjectAsync(id);
                return await mongoService.DeleteProjectAsync(id);
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
            catch (ViewNotFoundException)
            {
                logger.LogError(string.Format("Cannot retrieve project with id {0}", id));
                return StatusCode(StatusCodes.Status404NotFound, $"Cannot find project {id}");
            }
        }

        /// <summary>
        /// Get catalog of projects (MongoDB)
        /// </summary>
        /// <param name="userAdded">The username that created the project</param>
        /// <returns>Return a list of MongoProject class</returns>
        /// <response code="200">Returns the projects catalogue</response>
        /// <response code="408">If the request times out</response>
        [HttpGet("GetUserProject")]
        public async Task<ActionResult<List<MongoProject>>> GetProjectCatalog([FromQuery] string? userAdded)
        {
            try
            {
                if (!string.IsNullOrEmpty(userAdded))
                {
                    return await mongoService.GetAllProjectsByUserAsync(userAdded);
                }
                else
                {
                    return await mongoService.GetAllProjectsAsync();
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogError("Task was canceled due to timeout.");
                return StatusCode(StatusCodes.Status408RequestTimeout, "Request timed out.");
            }
        }
    }
}