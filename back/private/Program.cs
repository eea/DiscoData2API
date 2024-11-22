using System.Reflection;
using DiscoData2API_Priv.Services;
using DiscoData2API_Library.Class;
using Microsoft.OpenApi.Models;
using Serilog;

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
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "DiscoData API (Private)",
        Version = "v1",
        Description = "API for executing queries. ",
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

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();