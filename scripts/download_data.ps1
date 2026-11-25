[CmdletBinding()]
param(
    [string]$Url = "",
    [string]$S3Bucket = "",
    [string]$S3Prefix = "",
    [string]$EndpointUrl = "",
    [string]$Dest = "./data/landing",
    [switch]$VerifyOnly,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$RequiredDatasets = @(
    "clients",
    "projects",
    "survey_questions",
    "employees",
    "timesheets",
    "fx_rates",
    "invoices",
    "expenses",
    "crm_events",
    "employee_surveys"
)

if ([string]::IsNullOrWhiteSpace($Url) -and [string]::IsNullOrWhiteSpace($S3Bucket)) {
    if (-not [string]::IsNullOrWhiteSpace($env:DATASET_DOWNLOAD_URL)) {
        $Url = $env:DATASET_DOWNLOAD_URL
    }
    if (-not [string]::IsNullOrWhiteSpace($env:S3_BUCKET)) {
        $S3Bucket = $env:S3_BUCKET
    }
}

if ([string]::IsNullOrWhiteSpace($S3Prefix)) {
    $S3Prefix = if (-not [string]::IsNullOrWhiteSpace($env:S3_PREFIX)) { $env:S3_PREFIX } else { "landing" }
}

if ([string]::IsNullOrWhiteSpace($EndpointUrl) -and -not [string]::IsNullOrWhiteSpace($env:S3_ENDPOINT_URL)) {
    $EndpointUrl = $env:S3_ENDPOINT_URL
}

function Test-AllFoldersPopulated {
    param([string]$TargetDir)
    foreach ($ds in $RequiredDatasets) {
        $dsDir = Join-Path $TargetDir $ds
        if (-not (Test-Path -Path $dsDir -PathType Container)) {
            return $false
        }
        $firstFile = Get-ChildItem -Path $dsDir -File -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($null -eq $firstFile) {
            return $false
        }
    }
    return $true
}

function Test-ManifestChecksums {
    param([string]$TargetDir)

    $destManifest = Join-Path $TargetDir "MANIFEST.sha256"
    $parentDir = Split-Path -Path (Resolve-Path $TargetDir) -Parent
    $parentManifest = if ($parentDir) { Join-Path $parentDir "MANIFEST.sha256" } else { "" }

    $manifestPath = $null
    $baseDir = $null
    if (Test-Path -Path $destManifest -PathType Leaf) {
        $manifestPath = $destManifest
        $baseDir = (Resolve-Path $TargetDir).Path
    } elseif (-not [string]::IsNullOrWhiteSpace($parentManifest) -and (Test-Path -Path $parentManifest -PathType Leaf)) {
        $manifestPath = $parentManifest
        $baseDir = $parentDir
    } else {
        Write-Host "INFO: No MANIFEST.sha256 found; skipping checksum verification."
        return
    }

    Write-Host "Verifying SHA-256 checksums using $manifestPath..."
    $failures = 0
    foreach ($rawLine in Get-Content -Path $manifestPath) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) {
            continue
        }
        if ($line -match '^([a-fA-F0-9]{64})\s+\*?(.+)$') {
            $expectedHash = $matches[1].ToLowerInvariant()
            $relFile = $matches[2].Trim() -replace '/', [IO.Path]::DirectorySeparatorChar
            $targetFile = Join-Path $baseDir $relFile
            if (-not (Test-Path -Path $targetFile -PathType Leaf)) {
                [Console]::Error.WriteLine("  [FAIL] Missing file listed in MANIFEST.sha256: $targetFile")
                $failures++
                continue
            }
            $actualHash = (Get-FileHash -Path $targetFile -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($actualHash -ne $expectedHash) {
                [Console]::Error.WriteLine("  [FAIL] Checksum mismatch for ${relFile}: expected $expectedHash, got $actualHash")
                $failures++
            }
        }
    }
    if ($failures -gt 0) {
        throw "MANIFEST.sha256 verification failed with $failures error(s)."
    }
    Write-Host "MANIFEST.sha256 checksums verified successfully."
}

function Assert-RequiredFolders {
    param([string]$TargetDir)

    $missing = 0
    Write-Host ""
    Write-Host "Validating landing zone folders in ${TargetDir}:"
    foreach ($ds in $RequiredDatasets) {
        $dsDir = Join-Path $TargetDir $ds
        if (-not (Test-Path -Path $dsDir -PathType Container)) {
            Write-Host "  [MISSING] $ds/"
            $missing++
            continue
        }
        $files = @(Get-ChildItem -Path $dsDir -File -Recurse -ErrorAction SilentlyContinue)
        if ($files.Count -eq 0) {
            Write-Host "  [EMPTY]   $ds/"
            $missing++
        } else {
            Write-Host "  [OK]      $ds/ ($($files.Count) files)"
        }
    }

    if ($missing -gt 0) {
        throw "Landing zone validation failed. Expected all 10 non-empty dataset folders."
    }
    Write-Host ""
    Write-Host "SUCCESS: All 10 required datasets are present and verified."
}

if (-not $VerifyOnly) {
    if ((-not $Force) -and (Test-AllFoldersPopulated -TargetDir $Dest)) {
        Write-Host "INFO: All 10 dataset folders already exist in $Dest. Use -Force to re-download."
    } else {
        if ((-not [string]::IsNullOrWhiteSpace($Url)) -and (-not [string]::IsNullOrWhiteSpace($S3Bucket))) {
            throw "Specify either -Url or -S3Bucket, not both."
        }
        if ([string]::IsNullOrWhiteSpace($Url) -and [string]::IsNullOrWhiteSpace($S3Bucket)) {
            throw "No download source provided. Pass -Url <DATASET_DOWNLOAD_URL>, -S3Bucket <BUCKET>, or -VerifyOnly."
        }

        New-Item -ItemType Directory -Path $Dest -Force | Out-Null

        if (-not [string]::IsNullOrWhiteSpace($Url)) {
            $tmpDir = Join-Path ([IO.Path]::GetTempPath()) ("lakehouse_dl_" + [guid]::NewGuid().ToString("N"))
            $extractDir = Join-Path $tmpDir "extracted"
            New-Item -ItemType Directory -Path $extractDir -Force | Out-Null

            try {
                $cleanUrl = ($Url -split '\?')[0] -split '#' | Select-Object -First 1
                $isTarGz = $cleanUrl -match '\.(tar\.gz|tgz)$'
                $ext = if ($isTarGz) { ".tar.gz" } else { ".zip" }
                $archivePath = Join-Path $tmpDir ("landing_dataset" + $ext)

                Write-Host "Downloading dataset archive from $Url..."
                Invoke-WebRequest -Uri $Url -OutFile $archivePath -UseBasicParsing

                Write-Host "Extracting archive..."
                if ($isTarGz) {
                    tar -xzf $archivePath -C $extractDir
                    if ($LASTEXITCODE -ne 0) {
                        throw "tar extraction failed with exit code $LASTEXITCODE"
                    }
                } else {
                    Expand-Archive -Path $archivePath -DestinationPath $extractDir -Force
                }

                $sourceRoot = $extractDir
                if (Test-Path -Path (Join-Path $extractDir "landing\clients") -PathType Container) {
                    $sourceRoot = Join-Path $extractDir "landing"
                } elseif (Test-Path -Path (Join-Path $extractDir "data\landing\clients") -PathType Container) {
                    $sourceRoot = Join-Path $extractDir "data\landing"
                }

                Copy-Item -Path (Join-Path $sourceRoot "*") -Destination $Dest -Recurse -Force
                $rootManifest = Join-Path $extractDir "MANIFEST.sha256"
                $destManifest = Join-Path $Dest "MANIFEST.sha256"
                if (($sourceRoot -ne $extractDir) -and (Test-Path -Path $rootManifest -PathType Leaf) -and (-not (Test-Path -Path $destManifest -PathType Leaf))) {
                    $destParent = Split-Path -Path (Resolve-Path $Dest).Path -Parent
                    Copy-Item -Path $rootManifest -Destination (Join-Path $destParent "MANIFEST.sha256") -Force
                }
            } finally {
                Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
            }
        } else {
            if ($null -eq (Get-Command aws -ErrorAction SilentlyContinue)) {
                throw "aws CLI is required for -S3Bucket sync."
            }
            if ($S3Bucket.StartsWith("s3://")) {
                $s3Uri = $S3Bucket.TrimEnd("/")
            } elseif (-not [string]::IsNullOrWhiteSpace($S3Prefix)) {
                $s3Uri = "s3://$($S3Bucket.TrimEnd('/'))/$($S3Prefix.TrimStart('/'))"
            } else {
                $s3Uri = "s3://$($S3Bucket.TrimEnd('/'))"
            }

            $awsArgs = @("s3", "sync", $s3Uri, $Dest)
            if (-not [string]::IsNullOrWhiteSpace($EndpointUrl)) {
                $awsArgs += @("--endpoint-url", $EndpointUrl)
            }
            Write-Host "Syncing dataset from $s3Uri to $Dest..."
            & aws @awsArgs
            if ($LASTEXITCODE -ne 0) {
                throw "aws s3 sync failed with exit code $LASTEXITCODE"
            }
        }
    }
}

if (-not (Test-Path -Path $Dest -PathType Container)) {
    throw "Destination directory does not exist: $Dest"
}

Test-ManifestChecksums -TargetDir $Dest
Assert-RequiredFolders -TargetDir $Dest
