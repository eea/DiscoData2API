using DiscoData2API.Class;
using DiscoData2API.Misc;
using DiscoData2API.Services;
using DotNetEnv;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.OpenApi.Models;
using Prometheus;
using Serilog;
using System.IO.Compression;
using System.Reflection;

Env.Load("env");

//needs wwwrrot/swagger.yaml to exist
var builder = WebApplication.CreateBuilder(args);

// Add environment variables to Configuration
builder.Configuration.AddEnvironmentVariables();

// Configure Serilog for file logging
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("Logs/log-.txt", rollingInterval: RollingInterval.Day) // Logs with daily rolling interval
    .CreateLogger();

// Add Serilog to the logging system
builder.Host.UseSerilog();
builder.Services.Configure<ConnectionSettingsMongo>(builder.Configuration.GetSection("MongoSettings"));
builder.Services.AddSingleton<MongoService>();

// Dremio services — commented out, query execution moved to DuckDB sidecar
// builder.Services.Configure<ConnectionSettingsDremio>(builder.Configuration.GetSection("DremioSettings"));
// builder.Services.AddSingleton<FlightClientPool>();
// builder.Services.AddSingleton<QueryThrottlingService>();
// builder.Services.AddSingleton<CircuitBreakerService>();
// builder.Services.AddSingleton<DremioService>();

builder.Services.UseHttpClientMetrics();

builder.Services.AddResponseCompression(options =>
{
    options.EnableForHttps = true;
    options.MimeTypes = ["text/plain", "application/json", "text/json", "application/octet-stream", "Content-Disposition"];
});
builder.Services.Configure<GzipCompressionProviderOptions>
   (opt =>
   {
       opt.Level = CompressionLevel.SmallestSize;
   }
);


builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddHttpClient();
string[] swaggerControllerOrder = ["DataCatalog", "PublishedDataset", "SavedQuery"];
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "DiscoData API",
        Version = "v3.0",
        Description = @"API for the DiscoData published layer.<br>
        <strong>Owners</strong> — identity registration.<br>
        <strong>Published</strong> — catalog of published datasets (Parquet on object storage).<br>
        <strong>Saved Queries</strong> — user-saved DuckDB queries stored in MongoDB.",
    });

    options.AddSecurityDefinition(WriteKeyOperationFilter.SchemeId, new OpenApiSecurityScheme
    {
        Type = SecuritySchemeType.ApiKey,
        In = ParameterLocation.Header,
        Name = RequireWriteKeyAttribute.HeaderName,
        Description = "Shared key required for writes (saved queries, published catalog)."
    });
    options.OperationFilter<WriteKeyOperationFilter>();

    var xmlFilename = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
    options.IncludeXmlComments(Path.Combine(AppContext.BaseDirectory, xmlFilename));

    options.OrderActionsBy(api =>
    {
        var controllerIndex = Array.IndexOf(swaggerControllerOrder, api.ActionDescriptor.RouteValues["controller"]);
        var path = api.RelativePath ?? "";
        return $"{controllerIndex}_{path}";
    });

    options.TagActionsBy(api => api.ActionDescriptor.RouteValues["controller"] switch
    {
        "DataCatalog"      => ["Owners"],
        "PublishedDataset" => ["Published (Catalog)"],
        "SavedQuery"       => ["Saved Queries"],
        var other          => [other ?? "Other"]
    });
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
}

app.UseSwagger();
app.UseSwaggerUI(options =>
{
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
});

app.UseResponseCompression();
app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();
app.UseMetricServer();
app.UsePrometheusMiddleware();
app.Run();
