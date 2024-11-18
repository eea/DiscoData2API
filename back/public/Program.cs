using DiscoData2API.Misc;
using DiscoData2API.Services;
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

builder.Services.Configure<MongoSettings>(builder.Configuration.GetSection("MongoSettings"));
builder.Services.Configure<DremioSettings>(builder.Configuration.GetSection("DremioSettings"));
builder.Services.AddSingleton<DremioService>();
builder.Services.AddSingleton<MongoService>();
builder.Services.AddSingleton<DremioServiceBeta>();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
//builder.Services.AddSwaggerGen();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "Query API",
        Version = "v1",
        Description = "API for executing queries.",
    });
});

var app = builder.Build();


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    //app.UseSwaggerUI();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
    });
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();