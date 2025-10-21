# Requires: PowerShell 7+
# Usage examples:
#   pwsh .\Run-EnvOrchestrator.ps1 -BatchSize 3 -QuickChecks 2 -QuickDelaySec 5 -PollIntervalSec 15
#   pwsh .\Run-EnvOrchestrator.ps1 -RunTag 20251016-003800 -Resume
#   pwsh .\Run-EnvOrchestrator.ps1 -NoRemoteCleanup

[CmdletBinding()]
param(
  [string]$RunTag = $(Get-Date -Format 'yyyyMMdd-HHmmss'),
  [string]$ConfigFile = "connect_env_config.ps1",

  # LAUNCH PHASE
  [int]$BatchSize = 3,                 # how many environments to process per batch
  [int]$QuickChecks = 2,               # how many quick PID/exit checks after launch
  [int]$QuickDelaySec = 5,             # delay between those quick checks

  # WATCH PHASE
  [int]$PollIntervalSec = 15,          # delay between poll rounds
  [int]$TimeoutSec = 4*60*60,          # overall watch timeout

  # Misc
  [switch]$Resume,                     # do not upload/launch; only poll/download/cleanup
  [switch]$NoRemoteCleanup             # preserve /tmp/_run_<RunTag> per env
)

# -------------------------
# Load configuration
# -------------------------
$ConfigPath = [IO.Path]::Combine($PSScriptRoot,$ConfigFile)
if (-not (Test-Path -LiteralPath $ConfigPath)) {
  throw "Configuration file not found: $ConfigPath"
}
. $ConfigPath

# -------------------------
# Utilities
# -------------------------
function New-LocalDir {
  param([Parameter(Mandatory)][string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function Assert-FileExists {
  param([Parameter(Mandatory)][string]$Path, [string]$Hint = "")
  if (-not (Test-Path -LiteralPath $Path)) {
    if ($Hint) { throw "Required file not found: $Path ($Hint)" }
    throw "Required file not found: $Path"
  }
}

function New-UnixTextCopy {
  <#
    Normalizes text files to UTF-8 (no BOM) + LF line endings.
    Use this for *.sh / env files before scp.
  #>
  param([Parameter(Mandatory)][string]$Path)
  Assert-FileExists $Path
  [byte[]]$bytes = [IO.File]::ReadAllBytes($Path)
  if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
    $bytes = $bytes[3..($bytes.Length-1)]
  }
  $text = [Text.Encoding]::UTF8.GetString($bytes)
  $text = $text -replace "`r`n", "`n" -replace "`r", "`n"
  if (-not $text.EndsWith("`n")) { $text += "`n" }
  $tmp = [IO.Path]::Combine($env:TEMP, ("unix_" + [guid]::NewGuid() + [IO.Path]::GetExtension($Path)))
  $utf8NoBom = New-Object Text.UTF8Encoding($false)
  [IO.File]::WriteAllText($tmp, $text, $utf8NoBom)
  return $tmp
}

function Get-SshBaseArgs {
  param($EnvCfg)
  $envargs = @(
    "-T",
    "-p", $EnvCfg.Port.ToString(),
    "-o","BatchMode=yes",
    "-o","IdentitiesOnly=yes",
    "-o","ServerAliveInterval=60",
    "-o","ServerAliveCountMax=3",
    "-o","ConnectTimeout=10",
    "-o","StrictHostKeyChecking=no",
    "-o","UserKnownHostsFile=/dev/null",
    "-o","Ciphers=aes128-ctr",
    "-o","MACs=hmac-sha2-256",
    "$($EnvCfg.User)@$($EnvCfg.Host)"
  )
  if ($EnvCfg.Auth.Type -eq 'key' -and $EnvCfg.Auth.KeyPath) {
    $envargs = @("-i", $EnvCfg.Auth.KeyPath) + $envargs
  }
  return $envargs
}

function Invoke-SSH {
  <#
    Runs a remote command through ssh. We base64 the payload to dodge quoting.
    Returns: [pscustomobject] @{ ExitCode; StdOut; StdErr }
  #>
  param(
    [Parameter(Mandatory)]$EnvCfg,
    [Parameter(Mandatory)][string]$RemoteCommand
  )

  $base = Get-SshBaseArgs -EnvCfg $EnvCfg
  $payload    = ($RemoteCommand -replace "`r`n", "`n")
  $encodedcmd = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload))
  $remote     = "bash --noprofile --norc -lc 'base64 -d <<< $encodedcmd | bash -s'"

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = "ssh"
  $psi.Arguments = ($base + @($remote)) -join ' '
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute = $false

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  [void]$proc.Start()
  $out = $proc.StandardOutput.ReadToEnd()
  $err = $proc.StandardError.ReadToEnd()
  $proc.WaitForExit()

  [pscustomobject]@{ ExitCode=$proc.ExitCode; StdOut=$out; StdErr=$err }
}

function Invoke-SCPUpload {
  param(
    [Parameter(Mandatory)]$EnvCfg,
    [Parameter(Mandatory)][string[]]$LocalPaths,
    [Parameter(Mandatory)][string]$RemoteDir
  )
  $tempDir = [IO.Path]::Combine($env:TEMP, ("push_" + [guid]::NewGuid()))
  New-Item -ItemType Directory -Path $tempDir | Out-Null
  $prepared = @()
  try {
    foreach ($p in $LocalPaths) {
      Assert-FileExists $p
      $dest = $p
      if ($p -match '\\cf\.exe$') {
        $dest = [IO.Path]::Combine($tempDir, "cf")  # rename on Windows → cf (no .exe)
        Copy-Item $p $dest -Force
      } elseif ($p -match '\.(sh|env|txt|cfg|conf|vars)$') {
        $san = New-UnixTextCopy $p
        $dest = [IO.Path]::Combine($tempDir, ([IO.Path]::GetFileName($p)))
        Copy-Item $san $dest -Force
      }
      $prepared += $dest
    }

    $opts = @("-T",
              "-o","BatchMode=yes",
              "-o","IdentitiesOnly=yes",
              "-o","StrictHostKeyChecking=no",
              "-o","UserKnownHostsFile=/dev/null",
              "-o","Ciphers=aes128-ctr",
              "-o","MACs=hmac-sha2-256")
    $envargs = @()
    if ($EnvCfg.Auth.Type -eq 'key' -and $EnvCfg.Auth.KeyPath) { $envargs += @("-i", $EnvCfg.Auth.KeyPath) }
    $envargs += @("-P", $EnvCfg.Port.ToString()) + $prepared + "$($EnvCfg.User)@$($EnvCfg.Host):$RemoteDir/"

    Write-Host "scp -> $($EnvCfg.Name):$RemoteDir" -ForegroundColor DarkGray
    & scp @opts -r @envargs | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "SCP upload failed (code $LASTEXITCODE)" }
  }
  finally {
    Remove-Item -Recurse -Force $tempDir -ErrorAction SilentlyContinue
  }
}

function Invoke-SCPDownload {
  param(
    [Parameter(Mandatory)]$EnvCfg,
    [Parameter(Mandatory)][string]$RemotePathGlob,
    [Parameter(Mandatory)][string]$LocalDir
  )
  New-LocalDir $LocalDir
  $opts = @("-T",
            "-o","BatchMode=yes",
            "-o","IdentitiesOnly=yes",
            "-o","StrictHostKeyChecking=no",
            "-o","UserKnownHostsFile=/dev/null",
            "-o","Ciphers=aes128-ctr",
            "-o","MACs=hmac-sha2-256")
  $envargs = @()
  if ($EnvCfg.Auth.Type -eq 'key' -and $EnvCfg.Auth.KeyPath) { $envargs += @("-i", $EnvCfg.Auth.KeyPath) }
  $envargs += @("-P", $EnvCfg.Port.ToString(), "$($EnvCfg.User)@$($EnvCfg.Host):$RemotePathGlob", $LocalDir)

  Write-Host "scp <- $RemotePathGlob" -ForegroundColor DarkGray
  & scp -r @opts @envargs | Out-Null
  if ($LASTEXITCODE -ne 0) { Write-Warning "SCP download returned code $LASTEXITCODE (may be 'no files')." }
}

function Build-EnvBlock {
  param([string]$PlatformName)
  $envBlock = ""
  if ($PlatformEnv -and $PlatformEnv.ContainsKey($PlatformName)) {
    foreach ($kv in $PlatformEnv[$PlatformName].GetEnumerator()) {
      $val = $kv.Value
      if ($val -eq '(prompt)') {
        $val = Read-Host -Prompt "Value for ${PlatformName}:$($kv.Key)"
      }
      $valEsc = $val -replace "'", "'\''"
      $envBlock += "export $($kv.Key)='$valEsc'; "
    }
  }
  ($envBlock -replace "`r`n", "`n")
}

function Get-RemoteScriptDetached {
  param(
    [string]$RemoteRunDir,
    [string]$PlatformName,
    [string]$Api,
    [string]$EnvBlock = "",
    [string]$SourceCmd = "",
    [string[]]$Commands = $null
  )

  $cmdInner = if ($Commands -and $Commands.Count -gt 0) {
    ($Commands -join " && ")
  } else {
    "./inf.sh '$Api'"
  }

$tmpl = @'
set -Eeuo pipefail
{{ENV_BLOCK}}
{{SOURCE_CMD}}

cd '{{REMOTE_DIR}}'
mkdir -p outputs/{{PLATFORM}}
export CF_HOME='{{REMOTE_DIR}}/.cf/{{PLATFORM}}'
chmod +x inf.sh cf || true
rm -f outputs/{{PLATFORM}}/pid outputs/{{PLATFORM}}/exit.code || true

# Use a quoted heredoc so the *outer* shell doesn't expand;
# the inner bash will execute/expand it normally.
nohup bash --noprofile --norc >"outputs/{{PLATFORM}}/run.out" 2>"outputs/{{PLATFORM}}/run.err" <<'__RUN__' & echo $! > "outputs/{{PLATFORM}}/pid"
set -Eeuo pipefail
trap 'ec=$?; ts=$(date "+%F %T"); src="${BASH_SOURCE[0]:-$0}"; fn="${FUNCNAME[0]:-main}";
      echo "[$ts] ERROR ${ec:-1} at ${src}:${LINENO}: ${fn}: ${BASH_COMMAND}" >&2;
      echo "${ec:-1}" > "outputs/{{PLATFORM}}/exit.code";
      exit "${ec:-1}"' ERR
trap 'ec=$?; echo "${ec:-0}" > "outputs/{{PLATFORM}}/exit.code"' EXIT

# Optional CF login if env exports exist
if [ -n "${CF_API:-}" ] && [ -n "${CF_USERNAME:-}" ] && [ -n "${CF_PASSWORD:-}" ] ; then
  cf login -a "$CF_API" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o system -s system
fi

{{CMD_INNER}}
__RUN__
'@

  # Literal replacements (no PowerShell interpolation)
  $tmpl = $tmpl.Replace('{{ENV_BLOCK}}', $EnvBlock)
  $tmpl = $tmpl.Replace('{{SOURCE_CMD}}', $SourceCmd)
  $tmpl = $tmpl.Replace('{{REMOTE_DIR}}', $RemoteRunDir)
  $tmpl = $tmpl.Replace('{{PLATFORM}}', $PlatformName)
  $tmpl = $tmpl.Replace('{{CMD_INNER}}', $cmdInner)
  return $tmpl
}

function QuickProbe-Platform {
  param(
    $EnvCfg, [string]$RemoteRunDir, [string]$PlatformName,
    [int]$Checks = 2, [int]$DelaySec = 5
  )

$probeTemplate = @'
if [ ! -d '<<RD>>/outputs/<<PL>>' ]; then echo __MISSING__; exit; fi
if [ -f '<<RD>>/outputs/<<PL>>/exit.code' ]; then cat '<<RD>>/outputs/<<PL>>/exit.code'; exit; fi
if [ -f '<<RD>>/outputs/<<PL>>/pid' ]; then
  pid=$(cat '<<RD>>/outputs/<<PL>>/pid' 2>/dev/null)
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo __RUNNING__; exit; fi
fi
echo __PENDING__
'@

  $probeCmd = $probeTemplate.Replace('<<RD>>', $RemoteRunDir).Replace('<<PL>>', $PlatformName)

  for ($i=0; $i -lt $Checks; $i++) {
    $probe = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $probeCmd
    $val = $probe.StdOut.Trim()
    if ($val -ne '__PENDING__') { return $val }
    Start-Sleep -Seconds $DelaySec
  }
  return '__PENDING__'
}


function Check-PlatformStatus {
  param($EnvCfg, [string]$RemoteRunDir, [string]$PlatformName)

$statusTemplate = @'
if [ ! -d '<<RD>>/outputs/<<PL>>' ]; then echo STATE=Missing; exit; fi
if [ -f '<<RD>>/outputs/<<PL>>/exit.code' ]; then
  ec=$(cat '<<RD>>/outputs/<<PL>>/exit.code' 2>/dev/null)
  if [[ "$ec" =~ ^[0-9]+$ ]]; then echo STATE=Finished EC=$ec; else echo STATE=Finished EC=1; fi
  exit
fi
if [ -f '<<RD>>/outputs/<<PL>>/pid' ]; then
  pid=$(cat '<<RD>>/outputs/<<PL>>/pid' 2>/dev/null)
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo STATE=Running; exit; fi
fi
echo STATE=Running
'@

  $cmd = $statusTemplate.Replace('<<RD>>', $RemoteRunDir).Replace('<<PL>>', $PlatformName)
  $res = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $cmd
  $stdout = $res.StdOut.Trim()
  if ($stdout -match 'STATE=Missing') { return [pscustomobject]@{ State='Missing';  ExitCode=$null } }
  if ($stdout -match 'STATE=Finished EC=(\d+)') { return [pscustomobject]@{ State='Finished'; ExitCode=[int]$Matches[1] } }
  return [pscustomobject]@{ State='Running'; ExitCode=$null }
}


function Download-PlatformOutputs {
  param($EnvCfg, [string]$RemoteRunDir, [string]$PlatformName, [string]$LocalRoot)
  $localOut = [IO.Path]::Combine($LocalRoot, $PlatformName)
  New-LocalDir $localOut
  Invoke-SCPDownload -EnvCfg $EnvCfg -RemotePathGlob "$RemoteRunDir/outputs/$PlatformName/*" -LocalDir $localOut
}

# -------------------------
# Orchestration
# -------------------------

# Verify core files
Assert-FileExists $LocalFiles.CfBinary   "LocalFiles.CfBinary"
Assert-FileExists $LocalFiles.InfScript  "LocalFiles.InfScript"
New-LocalDir $OutRoot

$remoteRunDirByEnv = @{}     # envName -> /tmp/_run_<RunTag>
$pending = @()               # list of task records we’ll poll
$overallStart = Get-Date

# Build list of (Env, Platform, Api)
$work = foreach ($envCfg in $Environments) {
  foreach ($p in $envCfg.Platforms) {
    [pscustomobject]@{
      EnvCfg       = $envCfg
      EnvName      = $envCfg.Name
      PlatformName = $p.Name
      Api          = $p.Api
    }
  }
}

# Helper to prepare remote dir + uploads (idempotent)
function Ensure-Remote-Basics {
  param($EnvCfg, [string]$RemoteRunDir)
  # Create run dir
  $mk = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand "mkdir -p '$RemoteRunDir' && ls -ld '$RemoteRunDir'"
  if ($mk.ExitCode -ne 0) { throw "Failed to prepare remote dir for $($EnvCfg.Name): $($mk.StdErr)" }

  # Upload cf + inf.sh (only if missing)
  $need = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand @"
test -x '$RemoteRunDir/cf' && test -f '$RemoteRunDir/inf.sh' && echo OK || echo NEED
"@
  if ($need.StdOut.Trim() -eq 'NEED') {
    Invoke-SCPUpload -EnvCfg $EnvCfg -LocalPaths @($LocalFiles.CfBinary, $LocalFiles.InfScript) -RemoteDir $RemoteRunDir
    [void](Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand "chmod +x '$RemoteRunDir/cf' '$RemoteRunDir/inf.sh' || true")
  }
}

# LAUNCH PHASE (unless -Resume)
if (-not $Resume) {
  # walk envs in batches
  for ($i = 0; $i -lt $Environments.Count; $i += $BatchSize) {
    $batch = $Environments[$i..([Math]::Min($i+$BatchSize-1, $Environments.Count-1))]
    Write-Host ""
    Write-Host "=== LAUNCH BATCH: $($batch.Name -join ', ') ===" -ForegroundColor Cyan

    foreach ($envCfg in $batch) {
      $remoteRunDir = "/tmp/_run_$RunTag"
      $remoteRunDirByEnv[$envCfg.Name] = $remoteRunDir

      Ensure-Remote-Basics -EnvCfg $envCfg -RemoteRunDir $remoteRunDir

      foreach ($p in $envCfg.Platforms) {
        $platformName = $p.Name
        $api          = $p.Api

        # per-platform env exports + optional env file upload/source
        $envBlock = Build-EnvBlock -PlatformName $platformName

        $sourceCmd = ""
        if ($PlatformEnvFiles -and $PlatformEnvFiles.ContainsKey($platformName)) {
          $envFileLocal     = $PlatformEnvFiles[$platformName]
          $envFileSanitized = New-UnixTextCopy $envFileLocal
          Invoke-SCPUpload -EnvCfg $envCfg -LocalPaths @($envFileSanitized) -RemoteDir $remoteRunDir
          $remoteLeaf       = [IO.Path]::GetFileName($envFileLocal)
          $uploadedLeaf     = [IO.Path]::GetFileName($envFileSanitized)
          # move temp → final name
          [void](Invoke-SSH -EnvCfg $envCfg -RemoteCommand "mv -f '$remoteRunDir/$uploadedLeaf' '$remoteRunDir/$remoteLeaf' || true")
          $sourceCmd = "source '$remoteRunDir/$remoteLeaf'"
        }

        # optional custom commands
        [string[]]$commandsToRun = $null
        if ($PlatformCommands -and $PlatformCommands.ContainsKey($platformName)) {
          $val = $PlatformCommands[$platformName]
          if ($val -is [string]) { $commandsToRun = @($val) } else { $commandsToRun = $val }
        }

        # launch detached
        $script = Get-RemoteScriptDetached -RemoteRunDir $remoteRunDir -PlatformName $platformName -Api $api -EnvBlock $envBlock -SourceCmd $sourceCmd -Commands $commandsToRun
        $launch = Invoke-SSH -EnvCfg $envCfg -RemoteCommand $script
        if ($launch.ExitCode -ne 0) {
          Write-Warning "Launch failed on $($envCfg.Name)/${platformName}: $($launch.StdErr)"
          continue
        }

        # quick probe
        $qp = QuickProbe-Platform -EnvCfg $envCfg -RemoteRunDir $remoteRunDir -PlatformName $platformName -Checks $QuickChecks -DelaySec $QuickDelaySec
        if ($qp -in '__RUNNING__','0','__PENDING__') {
          Write-Host ("  -> {0}/{1} launched: {2}" -f $envCfg.Name,$platformName,$qp) -ForegroundColor DarkGray
        } else {
          Write-Warning ("  -> {0}/{1} early status: {2}" -f $envCfg.Name,$platformName,$qp)
        }

        # register for watch phase
        $pending += [pscustomobject]@{
          EnvCfg       = $envCfg
          EnvName      = $envCfg.Name
          RemoteRunDir = $remoteRunDir
          PlatformName = $platformName
          Api          = $api
          Done         = $false
          ExitCode     = $null
        }
      }
    }
    # move to next batch (no waiting here other than quick checks)
  }
}
else {
  # RESUME: do not upload/launch; just register all env/platforms for polling
  foreach ($envCfg in $Environments) {
    $remoteRunDir = "/tmp/_run_$RunTag"
    $remoteRunDirByEnv[$envCfg.Name] = $remoteRunDir
    foreach ($p in $envCfg.Platforms) {
      $pending += [pscustomobject]@{
        EnvCfg       = $envCfg
        EnvName      = $envCfg.Name
        RemoteRunDir = $remoteRunDir
        PlatformName = $p.Name
        Api          = $p.Api
        Done         = $false
        ExitCode     = $null
      }
    }
  }
}

# WATCH PHASE
Write-Host ""
Write-Host "=== WATCH/WAIT (RunTag=$RunTag) ===" -ForegroundColor Cyan
$start = Get-Date

while ($true) {
  $remaining = $pending | Where-Object { -not $_.Done }
  if (-not $remaining) { break }

  foreach ($task in $remaining) {
    # If remote run dir missing (e.g., cleaned up outside), mark invalid and advise
    $dirCheck = Invoke-SSH -EnvCfg $task.EnvCfg -RemoteCommand "test -d '$($task.RemoteRunDir)' && echo OK || echo MISS"
    if ($dirCheck.StdOut.Trim() -eq 'MISS') {
      Write-Warning ("[{0}] {1}/{2}: remote run dir missing for tag {3}. Treating as invalid. Check your local outputs." -f (Get-Date), $task.EnvName, $task.PlatformName, $RunTag)
      $task.Done = $true
      $task.ExitCode = $null
      continue
    }

    $st = Check-PlatformStatus -EnvCfg $task.EnvCfg -RemoteRunDir $task.RemoteRunDir -PlatformName $task.PlatformName
    switch ($st.State) {
      'Finished' {
        # download outputs then mark done
        $localRoot = [IO.Path]::Combine($OutRoot, [IO.Path]::Combine($task.EnvName, $task.PlatformName))
        Download-PlatformOutputs -EnvCfg $task.EnvCfg -RemoteRunDir $task.RemoteRunDir -PlatformName $task.PlatformName -LocalRoot $localRoot
        $task.Done = $true
        $task.ExitCode = $st.ExitCode
        Write-Host ("[{0}] {1}/{2} -> exit {3}" -f (Get-Date).ToString('HH:mm:ss'), $task.EnvName, $task.PlatformName, $st.ExitCode)
      }
      'Missing' {
        Write-Warning ("[{0}] {1}/{2}: outputs folder missing; run may have been manually removed." -f (Get-Date).ToString('HH:mm:ss'), $task.EnvName, $task.PlatformName)
        $task.Done = $true
      }
      default {
        # Running: nothing to do
      }
    }
  }

  if ((Get-Date) - $start -gt [TimeSpan]::FromSeconds($TimeoutSec)) {
    Write-Warning "Watch timeout hit after $TimeoutSec seconds."
    break
  }

  Start-Sleep -Seconds $PollIntervalSec
}

# CLEANUP (only when not disabled; and only if remote dir still exists)
if (-not $NoRemoteCleanup -and -not $Resume) {
  foreach ($envCfg in $Environments) {
    $rrd = "/tmp/_run_$RunTag"
    $check = Invoke-SSH -EnvCfg $envCfg -RemoteCommand "test -d '$rrd' && echo OK || echo MISS"
    if ($check.StdOut.Trim() -eq 'OK') {
      [void](Invoke-SSH -EnvCfg $envCfg -RemoteCommand "rm -rf '$rrd'")
      Write-Host "Cleaned $($envCfg.Name): $rrd" -ForegroundColor DarkGray
    }
  }
} else {
  Write-Host "Remote workspaces preserved (either -NoRemoteCleanup or -Resume)." -ForegroundColor DarkGray
}

# SUMMARY
Write-Host ""
Write-Host "=== SUMMARY (RunTag=$RunTag) ===" -ForegroundColor Cyan
foreach ($task in $pending) {
  $status = if ($task.Done) { "done (ec=$($task.ExitCode))" } else { "still running" }
  Write-Host ("{0}/{1}: {2}" -f $task.EnvName, $task.PlatformName, $status)
}
Write-Host ("Outputs root: {0}" -f $OutRoot)