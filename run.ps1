# SQL Converter Docker Build and Run Script
# This script builds and runs the SQL Server to DuckDB converter

Write-Host "Building Docker image..." -ForegroundColor Green
docker build -t sql-converter:latest .

if ($LASTEXITCODE -eq 0) {
    Write-Host "Build successful!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Running SQL converter..." -ForegroundColor Cyan
    Write-Host "================================"
    docker run --memory="12g" --rm -v "C:\Code\test:/app/data" sql-converter:latest
} else {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}