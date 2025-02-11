using System.Reflection;
using DiscoData2API_Priv.Services;
using DiscoData2API_Priv.Class;
using Microsoft.OpenApi.Models;
using Serilog;
using Microsoft.AspNetCore.ResponseCompression;
using System.IO.Compression;

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
        Title = "DiscoData API (Private)",
        Version = "v1",
        Description = @"Private API for Querying S3 Parquets via Dremio. <br>This API allows you to create, read, update, and delete SQL-based queries. <br>Queries are stored in MongoDB with a unique identifier.",
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

app.Run();