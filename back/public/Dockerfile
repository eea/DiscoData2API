# Use the official .NET Core runtime as the base image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
ARG ENVIRONMENT
EXPOSE 5205

# Use the official .NET Core SDK as the build image
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG ENVIRONMENT
WORKDIR /src

# Copy the class library project file
#COPY ["../classLibrary/DiscoData2API.csproj", "classLibrary/"]

# Copy the project file (relative to the Dockerfile's location in "public")
COPY ["public/DiscoData2API.csproj", "public/"]

# Restore dependencies
RUN dotnet restore "public/DiscoData2API.csproj"

# Copy the rest of the class library source files
#COPY classLibrary/ ./classLibrary/
# copy appropriate environment configuration
COPY ./Configs/appsettings_$ENVIRONMENT.json  ./appsettings.json


# Copy the rest of the application
COPY . .

# Build the application
WORKDIR "/src"
RUN dotnet build "public/DiscoData2API.csproj" -c Release -o /app/build

# Publish the application
FROM build AS publish
RUN dotnet publish "public/DiscoData2API.csproj" -c Release -o /app/publish

# Build the final image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_ENVIRONMENT=Development
ENV ASPNETCORE_URLS=http://+:5205
ENTRYPOINT ["dotnet", "DiscoData2API.dll"]
