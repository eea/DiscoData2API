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
builder.Services.Configure<ConnectionSettingsDremio>(builder.Configuration.GetSection("DremioSettings"));
builder.Services.AddSingleton<FlightClientPool>();
builder.Services.AddSingleton<QueryThrottlingService>();
builder.Services.AddSingleton<CircuitBreakerService>();
builder.Services.AddSingleton<DremioService>();
builder.Services.AddSingleton<MongoService>();
builder.Services.AddSingleton<DremioServiceBeta>();
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
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "DiscoData API (Public)",
        Version = "v2.0",
        Description = @"Public API for Querying Dremio DiscoData.<br>
        This API allows you to browse folders and Virtual Datasets (VDS) available in the DiscoData Gold space, inspect their schema, and query them with optional field selection and filters.<br>
        <strong>Note:</strong> The legacy Views endpoints are deprecated and will be removed in a future release. Use the <em>/api/data-product</em> endpoints instead.",
    });

    var xmlFilename = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
    options.IncludeXmlComments(Path.Combine(AppContext.BaseDirectory, xmlFilename));
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
}

app.UseSwagger();
//app.UseSwaggerUI();
app.UseSwaggerUI(options =>
{
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
});

app.UseResponseCompression();
app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();
app.UseMetricServer();
//Metrics.SuppressDefaultMetrics();
app.UsePrometheusMiddleware();
app.Run();