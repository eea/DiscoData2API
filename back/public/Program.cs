using DiscoData2API.Misc;
using DiscoData2API.Services;
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
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseStaticFiles(); //added for swagger yaml file


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
    // app.UseSwaggerUI(c =>
    // {
    //     c.SwaggerEndpoint("/swagger.yaml", "API V1"); //added for swagger yaml file
    // });
}


app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();