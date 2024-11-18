# Use the official .NET Core runtime as the base image
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

# Use the official .NET Core SDK as the build image
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy the project file and restore dependencies
COPY ["back/public/DiscoData2Api.csproj", "./"]
RUN dotnet restore "./DiscoData2Api.csproj"

# Copy the entire project and build it
COPY . .
WORKDIR "/src/back/public"
RUN dotnet build "DiscoData2Api.csproj" -c Release -o /app/build

# Publish the application
FROM build AS publish
RUN dotnet publish "DiscoData2Api.csproj" -c Release -o /app/publish

# Build the final image using the base image and the published output
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DiscoData2Api.dll"]
