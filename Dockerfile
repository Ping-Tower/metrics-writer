FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["src/Domain/Domain.csproj", "src/Domain/"]
COPY ["src/Application/Application.csproj", "src/Application/"]
COPY ["src/Infrastructure/Infrastructure.csproj", "src/Infrastructure/"]
COPY ["src/MetricsWriter/MetricsWriter.csproj", "src/MetricsWriter/"]

RUN dotnet restore "src/MetricsWriter/MetricsWriter.csproj"

COPY src ./src

WORKDIR /src/src/MetricsWriter
RUN dotnet publish "MetricsWriter.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "MetricsWriter.dll"]
