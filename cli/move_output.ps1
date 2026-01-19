param (
    [string]$BasePath = "work/B-1-1"
)

# Check if BasePath exists
if (-not (Test-Path $BasePath)) {
    Write-Host "Error: Base path '$BasePath' does not exist." -ForegroundColor Red
    exit 1
}

# 1. Identify the 'output' directory
$OutputDir = Join-Path $BasePath "output"
if (-not (Test-Path $OutputDir)) {
    Write-Host "Error: 'output' directory not found in '$BasePath'." -ForegroundColor Red
    exit 1
}

# 2. Find the hash directory inside 'output'
# We assume there is one directory like '07e4f431...'
$SubDirs = Get-ChildItem -Path $OutputDir -Directory

if ($SubDirs.Count -eq 0) {
    Write-Host "Error: No subdirectories found in '$OutputDir'." -ForegroundColor Red
    exit 1
}

# Iterate in case there are multiple, though seemingly there's just one target
foreach ($SourceDir in $SubDirs) {
    $DirName = $SourceDir.Name
    
    # 3. Determine Destination Directory (First 6 chars)
    if ($DirName.Length -lt 6) {
        $ShortName = $DirName
    } else {
        $ShortName = $DirName.Substring(0, 6)
    }
    
    $DestDir = Join-Path $BasePath $ShortName
    
    Write-Host "Processing: $($SourceDir.FullName)"
    Write-Host "Target:     $DestDir"
    
    # 4. Create Destination if it doesn't exist
    if (-not (Test-Path $DestDir)) {
        New-Item -ItemType Directory -Path $DestDir | Out-Null
        Write-Host "Created destination directory: $DestDir" -ForegroundColor Green
    }
    
    # 5. Move all files from Source to Destination
    Get-ChildItem -Path $SourceDir.FullName | Move-Item -Destination $DestDir -Force
    Write-Host "Moved files to $DestDir" -ForegroundColor Green
}

# 6. Remove the 'output' directory
Remove-Item -Path $OutputDir -Recurse -Force
Write-Host "Removed 'output' directory." -ForegroundColor Yellow
