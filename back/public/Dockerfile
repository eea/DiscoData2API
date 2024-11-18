# Use the official .NET Core runtime as the base image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 5205

# Use the official .NET Core SDK as the build image
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy the project file (relative to the Dockerfile's location in "public")
COPY ["DiscoData2API.csproj", "./"]

# Restore dependencies
RUN dotnet restore "./DiscoData2API.csproj"

# Copy the rest of the application
COPY . .

# Build the application
WORKDIR "/src"
RUN dotnet build "DiscoData2API.csproj" -c Release -o /app/build

# Publish the application
FROM build AS publish
RUN dotnet publish "DiscoData2API.csproj" -c Release -o /app/publish

# Build the final image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_ENVIRONMENT=Development
ENV ASPNETCORE_URLS=http://+:5205
ENTRYPOINT ["dotnet", "DiscoData2API.dll"]
