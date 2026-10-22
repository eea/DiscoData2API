using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace DiscoData2API.Misc
{
    /// <summary>
    /// Requires the shared write key in the X-Api-Key header. Fails closed: if WriteApiKey is not
    /// configured (empty, or a deploy placeholder like __WRITE_API_KEY__ that was never replaced),
    /// every write is refused with 503.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
    public class RequireWriteKeyAttribute : Attribute, IAuthorizationFilter
    {
        public const string HeaderName = "X-Api-Key";
        public const string ConfigKey = "WriteApiKey";

        public void OnAuthorization(AuthorizationFilterContext context)
        {
            var services = context.HttpContext.RequestServices;
            var expected = services.GetRequiredService<IConfiguration>()[ConfigKey];

            if (string.IsNullOrWhiteSpace(expected) || (expected.StartsWith("__") && expected.EndsWith("__")))
            {
                services.GetRequiredService<ILogger<RequireWriteKeyAttribute>>()
                    .LogError("{ConfigKey} is not configured; refusing write to {Path}", ConfigKey, context.HttpContext.Request.Path);
                context.Result = new ObjectResult("Writes are disabled: the write key is not configured.") { StatusCode = StatusCodes.Status503ServiceUnavailable };
                return;
            }

            var provided = context.HttpContext.Request.Headers[HeaderName].ToString();
            if (!CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(provided), Encoding.UTF8.GetBytes(expected)))
                context.Result = new UnauthorizedObjectResult($"Missing or invalid {HeaderName} header.");
        }
    }

    /// <summary>Marks [RequireWriteKey] operations in Swagger so the UI sends the key for them only.</summary>
    public class WriteKeyOperationFilter : IOperationFilter
    {
        public const string SchemeId = "WriteKey";

        public void Apply(OpenApiOperation operation, OperationFilterContext context)
        {
            var required = context.MethodInfo.GetCustomAttributes(true).OfType<RequireWriteKeyAttribute>().Any()
                || (context.MethodInfo.DeclaringType?.GetCustomAttributes(true).OfType<RequireWriteKeyAttribute>().Any() ?? false);
            if (!required)
                return;

            operation.Responses.TryAdd("401", new OpenApiResponse { Description = $"Missing or invalid {RequireWriteKeyAttribute.HeaderName} header" });
            operation.Security.Add(new OpenApiSecurityRequirement
            {
                [new OpenApiSecurityScheme { Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = SchemeId } }] = []
            });
        }
    }
}
