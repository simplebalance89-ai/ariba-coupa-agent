#requires -Modules Az.Storage
<#
.SYNOPSIS
    Pull CISM files from Azure Blob to local P21 import folder.
    
.DESCRIPTION
    Scheduled job (SQL Agent or Task Scheduler) that:
    1. Lists approved CISM files in Azure Blob
    2. Downloads to local P21 CISM import folder
    3. Moves blobs to archive in Azure
    4. Logs all operations
    
    Run every 5 minutes on the P21 server.
    
.PARAMETER StorageAccountName
    Azure Storage Account name (default: enproaidatav1)
    
.PARAMETER ContainerName
    Blob container name (default: ariba-coupa)
    
.PARAMETER LocalImportPath
    Local path where P21 CISM import watches (default: C:\P21\CISM\Import\Incoming\)
    
.PARAMETER LogPath
    Path to log file (default: C:\P21\CISM\Logs\blob_pull.log)
#>

param(
    [string]$StorageAccountName = "enproaidatav1",
    [string]$ContainerName = "ariba-coupa",
    [string]$LocalImportPath = "C:\P21\CISM\Import\Incoming\",
    [string]$LogPath = "C:\P21\CISM\Logs\blob_pull.log",
    [string]$StorageAccountKey = $env:AZURE_STORAGE_KEY
)

# ── Setup ─────────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

# Ensure directories exist
if (-not (Test-Path $LocalImportPath)) {
    New-Item -ItemType Directory -Path $LocalImportPath -Force | Out-Null
}

$LogDir = Split-Path $LogPath -Parent
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $logLine = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | $Level | $Message"
    Add-Content -Path $LogPath -Value $logLine
    if ($Level -eq "ERROR") { Write-Error $Message }
    else { Write-Host $logLine }
}

Write-Log "=== CISM Blob Pull Started ==="

# ── Azure Connection ──────────────────────────────────────────────────────────

try {
    if (-not $StorageAccountKey) {
        # Try to get from environment or Azure context
        if (Get-Command Get-AzContext -ErrorAction SilentlyContinue) {
            $ctx = Get-AzContext
            if (-not $ctx) {
                Write-Log "Not logged into Azure. Run Connect-AzAccount first." "ERROR"
                exit 1
            }
        } else {
            Write-Log "Az.Storage module not loaded and no storage key provided." "ERROR"
            exit 1
        }
        
        # Use current Azure context
        $storageContext = (Get-AzStorageAccount -ResourceGroupName "enpro-ai" -Name $StorageAccountName).Context
    } else {
        # Use storage key
        $storageContext = New-AzStorageContext -StorageAccountName $StorageAccountName -StorageAccountKey $StorageAccountKey
    }
    
    Write-Log "Connected to storage account: $StorageAccountName"
} catch {
    Write-Log "Failed to connect to Azure Storage: $_" "ERROR"
    exit 1
}

# ── Process Approved Files ────────────────────────────────────────────────────

$processedCount = 0
$failedCount = 0

try {
    # List blobs in approved/ folder
    $blobs = Get-AzStorageBlob -Container $ContainerName -Prefix "approved/" -Context $storageContext
    
    Write-Log "Found $($blobs.Count) file(s) in approved/"
    
    foreach ($blob in $blobs) {
        $blobName = $blob.Name
        $fileName = [System.IO.Path]::GetFileName($blobName)
        
        # Skip if not a .txt file (CISM format)
        if (-not $fileName.EndsWith(".txt", [System.StringComparison]::OrdinalIgnoreCase)) {
            Write-Log "Skipping non-CISM file: $blobName" "WARN"
            continue
        }
        
        $localFilePath = Join-Path $LocalImportPath $fileName
        $archiveBlobName = "archive/$fileName"
        
        try {
            Write-Log "Processing: $fileName ($($blob.Length) bytes)"
            
            # Download to local import folder
            Get-AzStorageBlobContent `
                -Container $ContainerName `
                -Blob $blobName `
                -Destination $localFilePath `
                -Context $storageContext `
                -Force
            
            Write-Log "Downloaded to: $localFilePath"
            
            # Verify file exists locally
            if (Test-Path $localFilePath) {
                $localSize = (Get-Item $localFilePath).Length
                
                if ($localSize -eq $blob.Length) {
                    # Archive the blob (copy then delete)
                    Start-AzStorageBlobCopy `
                        -SrcContainer $ContainerName `
                        -SrcBlob $blobName `
                        -DestContainer $ContainerName `
                        -DestBlob $archiveBlobName `
                        -Context $storageContext
                    
                    # Wait a moment for copy to complete
                    Start-Sleep -Milliseconds 500
                    
                    # Delete from approved/
                    Remove-AzStorageBlob `
                        -Container $ContainerName `
                        -Blob $blobName `
                        -Context $storageContext
                    
                    Write-Log "Archived: $blobName -> $archiveBlobName"
                    $processedCount++
                } else {
                    Write-Log "Size mismatch for $fileName. Expected $($blob.Length), got $localSize" "ERROR"
                    $failedCount++
                }
            } else {
                Write-Log "File not found after download: $localFilePath" "ERROR"
                $failedCount++
            }
        } catch {
            Write-Log "Failed to process $blobName : $_" "ERROR"
            $failedCount++
        }
    }
    
    Write-Log "=== Completed: $processedCount processed, $failedCount failed ==="
    
} catch {
    Write-Log "Error listing blobs: $_" "ERROR"
    exit 1
}

# Return exit code for SQL Agent job tracking
exit $failedCount
