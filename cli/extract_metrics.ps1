param (
    [Parameter(Mandatory=$true)]
    [string]$FilePath
)

# ファイルが存在するか確認
if (-not (Test-Path $FilePath)) {
    Write-Error "エラー: ファイル '$FilePath' が見つかりません。"
    exit 1
}

try {
    # JSONファイルを読み込んでパース
    $jsonContent = Get-Content -Path $FilePath -Raw -Encoding UTF8 | ConvertFrom-Json
}
catch {
    Write-Error "エラー: JSONファイルの読み込みまたはパースに失敗しました。"
    exit 1
}

# 'events' プロパティがあるか確認
if ($jsonContent.events) {
    $i = 1
    foreach ($event in $jsonContent.events) {
        if ($event.message) {
            try {
                # 'message' はJSON文字列なので、再度パースする
                # note: PowerShell 7前後で挙動が異なる場合があるため、明示的に文字列として扱う
                $messageData = $event.message | ConvertFrom-Json

                # 必要なフィールドを抽出してオブジェクトを作成
                $result = [PSCustomObject]@{
                    EventId            = $i
                    CpuTotalTime       = $messageData.cpu_total_time
                    MemoryUtilization  = $messageData.memory_utilization
                    Duration           = $messageData.duration
                }

                # 結果を出力 (リスト形式で見やすく)
                Write-Host "--- Event $i ---"
                $result | Format-List
                
                $i++
            }
            catch {
                Write-Warning "Event $i の message フィールドのパースに失敗しました: $_"
            }
        }
    }
}
else {
    Write-Warning "JSONに 'events' 配列が見つかりません。"
}
