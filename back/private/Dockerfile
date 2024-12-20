# Use the official .NET Core runtime as the base image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 5243

# Use the official .NET Core SDK as the build image
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy the class library project file
COPY ["../classLibrary/DiscoData2API_Library.csproj", "classLibrary/"]

# Copy the project file (relative to the Dockerfile's location in "public")
COPY ["private/DiscoData2API_Priv.csproj", "private/"]

# Restore dependencies
RUN dotnet restore "private/DiscoData2API_Priv.csproj"

# Copy the rest of the class library source files
COPY classLibrary/ ./classLibrary/

# Copy the rest of the application
COPY . .

# Build the application
WORKDIR "/src"
RUN dotnet build "private/DiscoData2API_Priv.csproj" -c Release -o /app/build

# Publish the application
FROM build AS publish
RUN dotnet publish "private/DiscoData2API_Priv.csproj" -c Release -o /app/publish

# Build the final image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_ENVIRONMENT=Development
ENV ASPNETCORE_URLS=http://+:5243

# copy appropriate environment configuration
COPY Configs/appsettings_$ENVIRONMENT.json  appsettings.json


ENTRYPOINT ["dotnet", "DiscoData2API_Priv.dll"]
