param (
    [string]$WorkRoot = "work"
)

# Check if WorkRoot exists
if (-not (Test-Path $WorkRoot)) {
    Write-Host "Error: Work root '$WorkRoot' does not exist." -ForegroundColor Red
    exit 1
}

# Get all subdirectories in WorkRoot (e.g., B-1-1, C-1, D-2)
$TargetDirs = Get-ChildItem -Path $WorkRoot -Directory

if ($TargetDirs.Count -eq 0) {
    Write-Host "No subdirectories found in '$WorkRoot'." -ForegroundColor Yellow
    exit 0
}

foreach ($TargetDir in $TargetDirs) {
    $BasePath = $TargetDir.FullName
    Write-Host "Checking: $BasePath" -ForegroundColor Cyan

    # 1. Identify the 'output' directory
    $OutputDir = Join-Path $BasePath "output"
    
    if (-not (Test-Path $OutputDir)) {
        Write-Host "  Skipping: No 'output' folder found." -ForegroundColor DarkGray
        continue
    }

    # 2. Find the hash directory inside 'output'
    $SubDirs = Get-ChildItem -Path $OutputDir -Directory

    if ($SubDirs.Count -eq 0) {
        Write-Host "  Warning: 'output' folder is empty." -ForegroundColor Yellow
        continue
    }

    foreach ($SourceDir in $SubDirs) {
        $DirName = $SourceDir.Name
        
        # 3. Determine Destination Directory (First 6 chars)
        if ($DirName.Length -lt 6) {
            $ShortName = $DirName
        } else {
            $ShortName = $DirName.Substring(0, 6)
        }
        
        $DestDir = Join-Path $BasePath $ShortName
        
        Write-Host "  Processing: $($SourceDir.Name)"
        Write-Host "  Target:     $ShortName"
        
        # 4. Create Destination if it doesn't exist
        if (-not (Test-Path $DestDir)) {
            New-Item -ItemType Directory -Path $DestDir | Out-Null
            Write-Host "  Created: $DestDir" -ForegroundColor Green
        }
        
        # 5. Move all files from Source to Destination
        Get-ChildItem -Path $SourceDir.FullName | Move-Item -Destination $DestDir -Force
        Write-Host "  Moved files to: $DestDir" -ForegroundColor Green
    }

    # 6. Remove the 'output' directory
    Remove-Item -Path $OutputDir -Recurse -Force
    Write-Host "  Removed 'output' directory." -ForegroundColor Yellow
}

Write-Host "Done processing all directories in $WorkRoot." -ForegroundColor Green
