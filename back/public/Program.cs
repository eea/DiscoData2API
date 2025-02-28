using System.Reflection;
using DiscoData2API.Services;
using DiscoData2API.Class;
using Microsoft.OpenApi.Models;
using Serilog;
using Prometheus;
using DiscoData2API.Misc;
using Microsoft.AspNetCore.ResponseCompression;
using System.IO.Compression;

//needs wwwrrot/swagger.yaml to exist
var builder = WebApplication.CreateBuilder(args);

// Configure Serilog for file logging
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("Logs/log-.txt", rollingInterval: RollingInterval.Day) // Logs with daily rolling interval
    .CreateLogger();

// Add Serilog to the logging system
builder.Host.UseSerilog();
builder.Services.Configure<ConnectionSettingsMongo>(builder.Configuration.GetSection("MongoSettings"));
builder.Services.Configure<ConnectionSettingsDremio>(builder.Configuration.GetSection("DremioSettings"));
builder.Services.AddSingleton<DremioService>();
builder.Services.AddSingleton<MongoService>();
builder.Services.AddSingleton<DremioServiceBeta>();
builder.Services.UseHttpClientMetrics();

builder.Services.AddResponseCompression(options =>
{
    options.EnableForHttps = true;
    options.MimeTypes = new[] { "text/plain", "application/json", "text/json", "application/octet-stream", "Content-Disposition" };
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
        Version = "v1.0",
        Description = @"Public API for Querying DataHub DiscoData,<br>
        This API allows you to retrieve a catalog of available Views (SQL-based queries). <br>
        To fetch data, use the unique identifier associated with a specific query.",
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
Metrics.SuppressDefaultMetrics();
app.UsePrometheusMiddleware();
app.Run();