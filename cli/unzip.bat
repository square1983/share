Get-ChildItem -Path "D:\data" -Recurse -Filter *.zip |
ForEach-Object {
    Expand-Archive -Path $_.FullName -DestinationPath $_.DirectoryName -Force
}
