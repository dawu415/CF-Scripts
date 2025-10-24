# Requires: PowerShell 7+
# Usage examples:
#   pwsh .\Run-EnvOrchestrator.ps1 -BatchSize 3 -QuickChecks 2 -QuickDelaySec 5 -PollIntervalSec 15
#   pwsh .\Run-EnvOrchestrator.ps1 -RunTag 20251016-003800 -Resume
#   pwsh .\Run-EnvOrchestrator.ps1 -NoRemoteCleanup

[CmdletBinding()]
param(
  [string]$RunTag = $(Get-Date -Format 'yyyyMMdd-HHmmss'),
  [string]$ConfigFile = "connect_env_config.ps1",

  [int]$SshHardTimeoutSec = 120,        # kill any SSH call that runs longer than this

  # LAUNCH PHASE
  [int]$BatchSize = 3,                 # how many environments to process per batch
  [int]$QuickChecks = 2,               # how many quick PID/exit checks after launch
  [int]$QuickDelaySec = 5,             # delay between those quick checks

  # WATCH PHASE
  [int]$PollIntervalSec = 15,          # delay between poll rounds
  [int]$TimeoutSec = 4*60*60,          # overall watch timeout
  [int]$StatusEvery = 1,               # print status-table every N rounds (1 = every round)
  [switch]$WatchClear,                 # clear screen before each table (classic "watch" style)

  # Debug/Tracing
  [switch]$DebugLaunch,                # save sanitized remote launch script to OutRoot\debug
  [switch]$ShowLaunchScript,           # also print sanitized launch script to console
  [switch]$TraceSSH,                   # trace ssh calls (no secrets)

  # Misc
  [switch]$Resume,                     # do not upload/launch; only poll/download/cleanup
  [switch]$NoRemoteCleanup             # preserve remote run dir per env
)

# -------------------------
# Load configuration
# -------------------------
$ConfigPath = [IO.Path]::Combine($PSScriptRoot,$ConfigFile)
if (-not (Test-Path -LiteralPath $ConfigPath)) {
  throw "Configuration file not found: $ConfigPath"
}
. $ConfigPath

# expose Trace flag to helpers
$script:TraceSSH = $TraceSSH

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

function Redact-Secrets {
  <#
    Redact likely secrets in shell fragments (env exports, args).
    We mask values for names containing PASSWORD|SECRET|TOKEN|KEY
  #>
  param([Parameter(Mandatory)][string]$Text)
  $out = New-Object System.Text.StringBuilder
  foreach ($line in ($Text -split "`r?`n")) {
    if ($line -match '^\s*export\s+([A-Z0-9_]+)=(.+)$') {
      $name = $matches[1]
      if ($name -match 'PASSWORD|SECRET|TOKEN|KEY') {
        $line = $line -replace '=(?:''.*?''|".*?"|\S+)', "='***REDACTED***'"
      }
    }
    # also scrub: -p "<pwd>" or CF_PASSWORD=<val>
    $line = [Regex]::Replace($line, '(?i)(\bCF_PASSWORD=)(\S+)', '$1***REDACTED***')
    $line = [Regex]::Replace($line, '(?i)(-p\s+)(["'']?).+?\2', '$1***REDACTED***')
    [void]$out.AppendLine($line)
  }
  $out.ToString()
}

function Get-SshBaseArgs {
  param($EnvCfg)
  $envargs = @(
    "-T",
    "-n",                      # <<< NEW: don't read from STDIN (prevents odd stalls)
    "-q",                       # quiet client output
    "-p", $EnvCfg.Port.ToString(),
    "-o","LogLevel=ERROR",      # suppress warnings/banners from the client
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

function Get-RemoteRunDir {
  param(
    [Parameter(Mandatory)] $EnvCfg,
    [Parameter(Mandatory)] [string] $RunTag
  )

  $pref = ""
  if ($EnvCfg.PSObject.Properties.Name -contains 'RemoteBase' -and $EnvCfg.RemoteBase) {
    $pref = $EnvCfg.RemoteBase
  }

  $probe = @'
RUN_TAG="__TAG__"
PREF="__PREF__"
set -Eeuo pipefail

dbg() { :; }
if [ -n "${CF_ORCH_TRACE:-}" ]; then
  dbg() { printf "DBG:%s\n" "$*"; }
fi

# ensure HOME exists; fall back if empty
if [ -z "${HOME:-}" ]; then
  HOME="$(getent passwd "$(id -un 2>/dev/null)" 2>/dev/null | awk -F: "{print \$6}")"
  dbg "HOME_fallback=$HOME"
fi

# order limited to what you want to allow
candidates=()
[ -n "$PREF" ] && candidates+=("$PREF")
candidates+=("$HOME/.cf_orch" "/tmp")

dbg "RUN_TAG=$RUN_TAG"
dbg "candidates=${candidates[*]}"

# prefer an existing run dir if present
for base in "${candidates[@]}"; do
  if [ -d "$base/_run_$RUN_TAG" ]; then
    dbg "reuse=$base"
    echo "$base"
    exit 0
  fi
done

for base in "${candidates[@]}"; do
  dbg "try=$base"

  # must be creatable and writable
  mkdir -p "$base" >/dev/null 2>&1 || { dbg "mkdir_fail=$base"; continue; }
  [ -w "$base" ] || { dbg "not_writable=$base"; continue; }

  # skip noexec mounts if detectable
  if command -v findmnt >/dev/null 2>&1; then
    if findmnt -no OPTIONS --target "$base" 2>/dev/null | grep -qw noexec; then
      dbg "noexec_mount=$base"
      continue
    fi
  fi

  # --- EXEC TEST: write & run a tiny script in-place ---
  t="$base/.exec_test_$$.sh"
  printf "#!/bin/sh\nexit 0\n" > "$t" 2>/dev/null || { dbg "write_fail=$base"; continue; }
  chmod +x "$t" >/dev/null 2>&1 || { rm -f "$t"; dbg "chmod_fail=$base"; continue; }
  if "$t" >/dev/null 2>&1; then
    rm -f "$t" >/dev/null 2>&1 || true
    dbg "exec_ok=$base"
    echo "$base"
    exit 0
  fi
  rm -f "$t" >/dev/null 2>&1 || true
  dbg "exec_fail=$base"
done

echo "__NOEXEC__"
'@

  $probe = $probe.Replace('__TAG__', $RunTag).Replace('__PREF__', $pref)

  $remoteWithTrace = if ($script:TraceSSH) { "export CF_ORCH_TRACE=1; $probe" } else { $probe }

  $res = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $remoteWithTrace -HardTimeoutSec $SshHardTimeoutSec
  if ($res.ExitCode -ne 0) {
    throw "Failed to probe remote base dir on $($EnvCfg.Name): $($res.StdErr)"
  }

  $raw   = ($res.StdOut -replace '\r\n', "`n")        # normalize
  $lines = $raw -split "\n"
  $nz    = $lines | Where-Object { $_ -and $_.Trim() }
  $base  = if ($nz) { ($nz | Select-Object -Last 1).Trim() } else { "" }

  if ([string]::IsNullOrWhiteSpace($base) -or $base -eq '__NOEXEC__') {
    $dbgTrail = ($lines | Where-Object { $_ -like 'DBG:*' }) -join [environment]::NewLine
    $hint = "No exec-capable writable directory found on $($EnvCfg.Name). " +
            "Set EnvCfg.RemoteBase (e.g. /home/<user>/.cf_orch) or ask ops for an exec-capable scratch dir."
    if ($dbgTrail) { throw "$hint`nProbe debug:`n$dbgTrail" } else { throw $hint }
  }

  return ([IO.Path]::Combine($base, "_run_$RunTag") -replace '\\','/')
}


function Invoke-SSH {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)]$EnvCfg,
    [Parameter(Mandatory)][string]$RemoteCommand,
    [int]$HardTimeoutSec = 0
  )

  $base = Get-SshBaseArgs -EnvCfg $EnvCfg
  $payload    = ($RemoteCommand -replace "`r`n", "`n")
  $encodedcmd = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload))

  $markerOut = "__CFORCH_OUT__"
  $markerErr = "__CFORCH_ERR__"

  # Print per-stream markers, then run the payload under a quiet bash with a minimal PATH.
  $remote = "bash --noprofile --norc -lc 'printf `"%s\n`" `"$markerOut`"; printf `"%s\n`" `"$markerErr`" 1>&2; " +
            "base64 -d <<< $encodedcmd | env -u BASH_ENV PATH=/usr/bin:/bin bash --noprofile --norc -s'"

  if ($script:TraceSSH) {
    $bytes = [Text.Encoding]::UTF8.GetByteCount($payload)
    Write-Host ("[TRACE] ssh {0}@{1}:{2} payload={3} bytes" -f $EnvCfg.User, $EnvCfg.Host, $EnvCfg.Port, $bytes) -ForegroundColor DarkGray
  }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = "ssh"
  $psi.Arguments = ($base + @($remote)) -join ' '
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute = $false

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  [void]$proc.Start()

  if ($HardTimeoutSec -gt 0) {
    if (-not $proc.WaitForExit($HardTimeoutSec * 1000)) {
      try { $proc.Kill() } catch { }
      [void]$proc.WaitForExit(3000)
      $out = $proc.StandardOutput.ReadToEnd()
      $err = "[HARD TIMEOUT] exceeded ${HardTimeoutSec}s`n" + $proc.StandardError.ReadToEnd()
      return [pscustomobject]@{ ExitCode = 255; StdOut = $out; StdErr = $err }
    }
  }

  $out = $proc.StandardOutput.ReadToEnd()
  $err = $proc.StandardError.ReadToEnd()
  $proc.WaitForExit()

  function Slice-AfterMarker([string]$txt, [string]$m) {
    if ([string]::IsNullOrEmpty($txt)) { return $txt }
    $idx = $txt.LastIndexOf($m)
    if ($idx -lt 0) { return $txt.Trim() }
    $start = $idx + $m.Length
    # Skip CR/LF/TAB/SPACE immediately after the marker
    while ($start -lt $txt.Length -and (([int][char]$txt[$start]) -in 9,10,13,32)) { $start++ }
    return $txt.Substring($start)
  }

  function Clean-AfterSlice([string]$s) {
    if ($null -eq $s) { return $s }
    $s = ($s -replace "`r`n","`n" -replace "`r","`n").Trim()
    # If the *very first* character is a stray literal 'n' and the next token is a control token, drop it
    if ($s -match '^(?:n)(__(?:RUNNING|PENDING|MISSING)__|STATE=.*|\d+|OK|MISS)$') { $s = $Matches[1] }
    return $s
  }

  $out = Clean-AfterSlice (Slice-AfterMarker $out $markerOut)
  $err = Clean-AfterSlice (Slice-AfterMarker $err $markerErr)

  return [pscustomobject]@{ ExitCode=$proc.ExitCode; StdOut=$out; StdErr=$err }
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

    $opts = @(
    "-T",
    "-q",                        # quiet scp: no progress meters/banners
    "-o","BatchMode=yes",
    "-o","IdentitiesOnly=yes",
    "-o","StrictHostKeyChecking=no",
    "-o","UserKnownHostsFile=/dev/null",
    "-o","Ciphers=aes128-ctr",
    "-o","MACs=hmac-sha2-256"
    )

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
  $opts = @(
    "-T",
    "-q",                        # quiet scp: no progress meters/banners
    "-o","BatchMode=yes",
    "-o","IdentitiesOnly=yes",
    "-o","StrictHostKeyChecking=no",
    "-o","UserKnownHostsFile=/dev/null",
    "-o","Ciphers=aes128-ctr",
    "-o","MACs=hmac-sha2-256"
  )
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
    [string[]]$Commands = $null,
    [string]$CfSession = "" 
  )

  $cfSess = if ([string]::IsNullOrWhiteSpace($CfSession)) { "default" } else { $CfSession }

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
umask 077

export CF_ORCH_RUN_DIR='{{REMOTE_DIR}}'
export CF_ORCH_PLATFORM='{{PLATFORM}}'
export CF_ORCH_OUT_DIR='{{REMOTE_DIR}}/outputs/{{PLATFORM}}'

export CF_HOME='{{REMOTE_DIR}}/.cf/{{PLATFORM}}/{{CF_SESSION}}'
mkdir -p "$CF_HOME"
chmod +x inf.sh cf || true
rm -f outputs/{{PLATFORM}}/pid outputs/{{PLATFORM}}/exit.code || true

# Record session id for debugging/resume visibility
echo '{{CF_SESSION}}' > "outputs/{{PLATFORM}}/cf_session"

# --- ensure our uploaded ./cf is found everywhere ---
export PATH='{{REMOTE_DIR}}':"$PATH"

# Use a quoted heredoc so the *outer* shell doesn't expand;
# the inner bash will execute/expand it normally.
nohup bash --noprofile --norc >"outputs/{{PLATFORM}}/run.out" 2>"outputs/{{PLATFORM}}/run.err" <<'__RUN__' & echo $! > "outputs/{{PLATFORM}}/pid"
set -Eeuo pipefail

# Make sure PATH is correct inside the child shell as well
export PATH="$PWD:$PATH"

trap 'ec=$?; ts=$(date "+%F %T"); src="${BASH_SOURCE[0]:-$0}"; fn="${FUNCNAME[0]:-main}";
      echo "[$ts] ERROR ${ec:-1} at ${src}:${LINENO}: ${fn}: ${BASH_COMMAND}" >&2;
      echo "${ec:-1}" > "outputs/{{PLATFORM}}/exit.code";
      exit "${ec:-1}"' ERR
trap 'ec=$?; echo "${ec:-0}" > "outputs/{{PLATFORM}}/exit.code"' EXIT

# Optional CF login if env exports exist
if [ -n "${CF_API:-}" ] && [ -n "${CF_USERNAME:-}" ] && [ -n "${CF_PASSWORD:-}" ] ; then
  org="${CF_ORG:-system}"; space="${CF_SPACE:-system}"
  cf login -a "$CF_API" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o "$org" -s "$space"
fi

{{CMD_INNER}}
__RUN__
'@

  $tmpl = $tmpl.Replace('{{ENV_BLOCK}}', $EnvBlock).
                Replace('{{SOURCE_CMD}}', $SourceCmd).
                Replace('{{REMOTE_DIR}}', $RemoteRunDir).
                Replace('{{PLATFORM}}', $PlatformName).
                Replace('{{CMD_INNER}}', $cmdInner).
                Replace('{{CF_SESSION}}', $cfSess) 

  return $tmpl
}

function Normalize-ControlToken {
  param([string]$s)
  if ($null -eq $s) { return '' }
  $s = ($s -replace "`r`n","`n" -replace "`r","`n").Trim()
  if ($s -match '^(?:n)(__(?:RUNNING|PENDING|MISSING)__|STATE=.*|\d+|OK|MISS)$') { $s = $Matches[1] }
  if ([string]::IsNullOrWhiteSpace($s)) { $s = '__PENDING__' }
  return $s
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
    $probe = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $probeCmd -HardTimeoutSec $SshHardTimeoutSec
    if ($probe.ExitCode -eq 255 -and $probe.StdErr -match 'HARD TIMEOUT') { return '__PENDING__' }
    $val = Normalize-ControlToken $probe.StdOut
    if ($val -ne '__PENDING__') { return $val }
    Start-Sleep -Seconds $DelaySec
  }
  return '__PENDING__'
}

function Check-PlatformStatus {
  <#
    Returns one of: Pending, Running, Finished, Missing, Timeout (+ ExitCode if finished)
  #>
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
echo STATE=Pending
'@

  $cmd = $statusTemplate.Replace('<<RD>>', $RemoteRunDir).Replace('<<PL>>', $PlatformName)
  $res = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $cmd -HardTimeoutSec $SshHardTimeoutSec

  if ($res.ExitCode -eq 255 -and $res.StdErr -match 'HARD TIMEOUT') {
    return [pscustomobject]@{ State='Timeout'; ExitCode=$null }
  }

  $stdout = Normalize-ControlToken $res.StdOut
  if ($stdout -match 'STATE=Missing')          { return [pscustomobject]@{ State='Missing';  ExitCode=$null } }
  if ($stdout -match 'STATE=Finished EC=(\d+)'){ return [pscustomobject]@{ State='Finished'; ExitCode=[int]$Matches[1] } }
  if ($stdout -match 'STATE=Running')          { return [pscustomobject]@{ State='Running';  ExitCode=$null } }
  return [pscustomobject]@{ State='Pending'; ExitCode=$null }
}

function Download-PlatformOutputs {
  param($EnvCfg, [string]$RemoteRunDir, [string]$PlatformName, [string]$LocalRoot)
  $localOut = [IO.Path]::Combine($LocalRoot, $PlatformName)
  New-LocalDir $localOut
  Invoke-SCPDownload -EnvCfg $EnvCfg -RemotePathGlob "$RemoteRunDir/outputs/$PlatformName/*" -LocalDir $localOut
}

function Show-StatusTable {
  param(
    [Parameter(Mandatory)] $Tasks,
    [Parameter(Mandatory)] [int] $Round,
    [Parameter(Mandatory)] [datetime] $Start,   # pass $overallStart here
    [switch]$Clear
  )
  if ($Clear) { Clear-Host }
  $now = Get-Date
  $elapsed = New-TimeSpan -Start $Start -End $now

  $total = $Tasks.Count
  $done  = ($Tasks | Where-Object { $_.Done }).Count
  $active= $total - $done

  Write-Host ("[{0}] RunTag={1} | Round #{2} | Total {3:hh\:mm\:ss} | Active {4}/{5}" `
              -f $now.ToString('HH:mm:ss'), $RunTag, $Round, $elapsed, $active, $total) -ForegroundColor Cyan

  # Build rows
  $rows = foreach ($t in $Tasks) {
    $disp = switch ($t.State) {
      'Finished' { "_COMPLETED_ (ec=$($t.ExitCode))" }
      'Running'  { '__RUNNING__' }
      'Pending'  { '__PENDING__' }
      'Missing'  { 'MISSING' }
      'Timeout'  { 'SSH_TIMEOUT' }
      default    { $t.State }
    }
    $since = if ($t.PSObject.Properties.Name -contains 'LastStateChange' -and $t.LastStateChange) {
      (New-TimeSpan -Start $t.LastStateChange -End $now).ToString("hh\:mm\:ss")
    } else { "" }
    [pscustomobject]@{ Env=$t.EnvName; Platform=$t.PlatformName; Status=$disp; LastChange=$since }
  }

  $rows = $rows | Sort-Object Env, Platform

  # Compute widths safely
  $envLens  = @(); $platLens = @()
  foreach ($r in $rows) { $envLens += $r.Env.Length; $platLens += $r.Platform.Length }
  $maxEnv  = if ($envLens.Count)  { ($envLens  | Measure-Object -Maximum).Maximum } else { 3 }
  $maxPlat = if ($platLens.Count) { ($platLens | Measure-Object -Maximum).Maximum } else { 8 }
  $wEnv  = [Math]::Min(30, [Math]::Max(3,  $maxEnv))
  $wPlat = [Math]::Min(24, [Math]::Max(8,  $maxPlat))
  $wStat = 28
  $fmt   = "{0,-$wEnv}  {1,-$wPlat}  {2,-$wStat}  {3,9}"

  Write-Host ($fmt -f 'Env','Platform','Status','LastChange')
  Write-Host ($fmt -f ('-'*$wEnv), ('-'*$wPlat), ('-'*$wStat), ('-'*9))
  foreach ($r in $rows) { Write-Host ($fmt -f $r.Env, $r.Platform, $r.Status, $r.LastChange) }
  Write-Host ""

  # Force the console to paint immediately
  [Console]::Out.Flush()
}

# -------------------------
# Orchestration
# -------------------------

# Verify core files
Assert-FileExists $LocalFiles.CfBinary   "LocalFiles.CfBinary"
Assert-FileExists $LocalFiles.InfScript  "LocalFiles.InfScript"
New-LocalDir $OutRoot

$remoteRunDirByEnv = @{}     # envName -> resolved run dir
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

function Assert-RemoteArtifacts {
  <#
    Verifies that inf.sh exists, cf is executable, and ./cf actually runs.
    Throws with rich diagnostics if anything is wrong.
  #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)] $EnvCfg,
    [Parameter(Mandatory)][string] $RemoteRunDir,
    [int] $SshHardTimeoutSec = 0
  )

  # Bash-safe single-quote escape for embedding paths inside single-quoted strings
  $rrdEsc = ($RemoteRunDir -replace "'", "'\''")

  $verifyCmd = @"
set -Eeuo pipefail
cd '$rrdEsc' || exit 2

# Ensure bits are set (idempotent)
chmod +x cf 2>/dev/null || true
chmod +x inf.sh 2>/dev/null || true

# Structural checks
[ -f inf.sh ] && [ -x cf ] || exit 3

# Prove it's runnable (catches noexec/ACL/loader issues)
"./cf" --version >/dev/null 2>&1 || exit 4
exit 0
"@

  $verify = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $verifyCmd -HardTimeoutSec $SshHardTimeoutSec
  if ($verify.ExitCode -ne 0) {
    # Deep diagnostics so the failure is actionable
    $diagCmd = @"
set +e
cd '$rrdEsc' 2>/dev/null || true

echo '--- id/uname ---'
id; uname -a

echo '--- mount options for run dir ---'
( command -v findmnt >/dev/null 2>&1 && findmnt -no TARGET,OPTIONS --target . ) || ( mount | head -n 10 )

echo '--- perms ---'
ls -ld .; ls -l inf.sh cf 2>&1

echo '--- stat ---'
( stat -c '%n %a %A %U:%G %s %F' inf.sh cf 2>/dev/null || stat -f '%N %p %Sp %Su:%Sg %z %HT' inf.sh cf 2>/dev/null ) || true

echo '--- ACL (if any) ---'
( getfacl -p inf.sh cf 2>/dev/null ) || true

echo '--- try ./cf --version ---'
"./cf" --version ; echo "EC=$?"
"@

    $diag = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $diagCmd -HardTimeoutSec $SshHardTimeoutSec
    throw "Remote artifacts check failed on $($EnvCfg.Name) under $RemoteRunDir.`nVerifyExit=$($verify.ExitCode)`n$($diag.StdOut)`n$($diag.StdErr)"
  }
}

# Helper to prepare remote dir + uploads (idempotent)
function Ensure-Remote-Basics {
  param($EnvCfg, [string]$RemoteRunDir)

  # 1) Make sure run dir exists
  $mk = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand "mkdir -p '$RemoteRunDir' && ls -ld '$RemoteRunDir'" -HardTimeoutSec $SshHardTimeoutSec
  if ($mk.ExitCode -ne 0) { throw "Failed to prepare remote dir for $($EnvCfg.Name): $($mk.StdErr)" }

  # 2) ALWAYS push both artifacts (simpler + robust)
  Invoke-SCPUpload -EnvCfg $EnvCfg -LocalPaths @($LocalFiles.CfBinary, $LocalFiles.InfScript) -RemoteDir $RemoteRunDir

  # 3) Normalize names & permissions on remote and show a quick verify when tracing
  $infLeafLocal = [IO.Path]::GetFileName($LocalFiles.InfScript)

  # Escape single quotes for safe embedding in bash single quotes
  function _Esc([string]$s) { $s -replace "'", "'\''" }
  $rrdEsc  = _Esc $RemoteRunDir
  $leafEsc = _Esc $infLeafLocal

  $fix = @'
set -Eeuo pipefail
cd '<<RRD>>'

# If the script isn't named exactly inf.sh, rename it so the launcher can ./inf.sh
if [ ! -f inf.sh ]; then
  if [ -f '<<INF_LEAF>>' ]; then
    mv -f '<<INF_LEAF>>' inf.sh
  else
    # as a fallback: if exactly one *.sh exists, make it inf.sh
    cnt=$(ls -1 *.sh 2>/dev/null | wc -l | tr -d ' ')
    if [ "$cnt" = "1" ]; then mv -f $(ls -1 *.sh) inf.sh; fi
  fi
fi

# If cf arrived as cf.exe, unify to cf
[ -f cf.exe ] && mv -f cf.exe cf

# Ensure exec bits
chmod +x cf 2>/dev/null || true
chmod +x inf.sh 2>/dev/null || true

# Show what we have (helps when -TraceSSH is on)
ls -l inf.sh cf 2>/dev/null || true
'@

  $fix = $fix.Replace('<<RRD>>', $rrdEsc).Replace('<<INF_LEAF>>', $leafEsc)

  $post = Invoke-SSH -EnvCfg $EnvCfg -RemoteCommand $fix -HardTimeoutSec $SshHardTimeoutSec
  if ($script:TraceSSH) {
    Write-Host "[TRACE] post-upload verify ($($EnvCfg.Name)):`n$($post.StdOut)" -ForegroundColor DarkGray
  }

  Assert-RemoteArtifacts -EnvCfg $EnvCfg -RemoteRunDir $RemoteRunDir -SshHardTimeoutSec $SshHardTimeoutSec
}


# LAUNCH PHASE (unless -Resume)
if (-not $Resume) {
  for ($i = 0; $i -lt $Environments.Count; $i += $BatchSize) {
    $batch = $Environments[$i..([Math]::Min($i+$BatchSize-1, $Environments.Count-1))]
    Write-Host ""
    Write-Host "=== LAUNCH BATCH: $($batch.Name -join ', ') ===" -ForegroundColor Cyan

    foreach ($envCfg in $batch) {
      $remoteRunDir = Get-RemoteRunDir -EnvCfg $envCfg -RunTag $RunTag
      $remoteRunDirByEnv[$envCfg.Name] = $remoteRunDir

      Ensure-Remote-Basics -EnvCfg $envCfg -RemoteRunDir $remoteRunDir

      foreach ($p in $envCfg.Platforms) {
        $platformName = $p.Name
        $api          = $p.Api

        $envBlock = Build-EnvBlock -PlatformName $platformName

        $sourceCmd = ""
        if ($PlatformEnvFiles -and $PlatformEnvFiles.ContainsKey($platformName)) {
          $envFileLocal     = $PlatformEnvFiles[$platformName]
          $envFileSanitized = New-UnixTextCopy $envFileLocal
          Invoke-SCPUpload -EnvCfg $envCfg -LocalPaths @($envFileSanitized) -RemoteDir $remoteRunDir
          $remoteLeaf       = [IO.Path]::GetFileName($envFileLocal)
          $uploadedLeaf     = [IO.Path]::GetFileName($envFileSanitized)
          [void](Invoke-SSH -EnvCfg $envCfg -RemoteCommand "mv -f '$remoteRunDir/$uploadedLeaf' '$remoteRunDir/$remoteLeaf' || true" -HardTimeoutSec $SshHardTimeoutSec)
          $sourceCmd = "source '$remoteRunDir/$remoteLeaf'"
        }

        [string[]]$commandsToRun = $null
        if ($PlatformCommands -and $PlatformCommands.ContainsKey($platformName)) {
          $val = $PlatformCommands[$platformName]
          if ($val -is [string]) { $commandsToRun = @($val) } else { $commandsToRun = $val }
        }

        $cfSession = "sess-$RunTag-$PID-$([Guid]::NewGuid().ToString('N').Substring(0,8))"

        $script = Get-RemoteScriptDetached -RemoteRunDir $remoteRunDir -PlatformName $platformName -Api $api -EnvBlock $envBlock -SourceCmd $sourceCmd -Commands $commandsToRun  -CfSession $cfSession 

        if ($DebugLaunch -or $ShowLaunchScript) {
          $safe = Redact-Secrets $script
          $dbgPath = [IO.Path]::Combine($OutRoot, "debug", $envCfg.Name, "$($platformName)-launch.sh")
          New-LocalDir ([IO.Path]::GetDirectoryName($dbgPath))
          [IO.File]::WriteAllText($dbgPath, $safe)
          if ($ShowLaunchScript) {
            Write-Host "----- BEGIN remote launch script ($($envCfg.Name)/$platformName) -----" -ForegroundColor Yellow
            Write-Host $safe
            Write-Host "----- END remote launch script -----" -ForegroundColor Yellow
          } else {
            Write-Host "Saved debug launch script → $dbgPath" -ForegroundColor DarkGray
          }
        }

        $launch = Invoke-SSH -EnvCfg $envCfg -RemoteCommand $script -HardTimeoutSec $SshHardTimeoutSec
        if ($launch.ExitCode -ne 0) {
          Write-Warning "Launch failed on $($envCfg.Name)/${platformName}: $($launch.StdErr)"
          continue
        }

        $qp = QuickProbe-Platform -EnvCfg $envCfg -RemoteRunDir $remoteRunDir -PlatformName $platformName -Checks $QuickChecks -DelaySec $QuickDelaySec
        if ($qp -in '__RUNNING__','0','__PENDING__') {
          Write-Host ("  -> {0}/{1} launched: {2}" -f $envCfg.Name,$platformName,$qp) -ForegroundColor DarkGray
        } elseif ($qp -match '^\d+$') {
          Write-Warning ("  -> {0}/{1} exited immediately with code {2}" -f $envCfg.Name,$platformName,$qp)
        } else {
          Write-Warning ("  -> {0}/{1} early status: {2}" -f $envCfg.Name,$platformName,$qp)
        }

        $pending += [pscustomobject]@{
          EnvCfg          = $envCfg
          EnvName         = $envCfg.Name
          RemoteRunDir    = $remoteRunDir
          PlatformName    = $platformName
          Api             = $api
          Done            = $false
          ExitCode        = $null
          State           = 'Pending'
          LastStateChange = Get-Date
        }
      }
    }
  }
}
else {
  foreach ($envCfg in $Environments) {
    $remoteRunDir = Get-RemoteRunDir -EnvCfg $envCfg -RunTag $RunTag
    $remoteRunDirByEnv[$envCfg.Name] = $remoteRunDir
    foreach ($p in $envCfg.Platforms) {
      $pending += [pscustomobject]@{
        EnvCfg          = $envCfg
        EnvName         = $envCfg.Name
        RemoteRunDir    = $remoteRunDir
        PlatformName    = $p.Name
        Api             = $p.Api
        Done            = $false
        ExitCode        = $null
        State           = 'Pending'
        LastStateChange = Get-Date
      }
    }
  }
}

# WATCH PHASE
Write-Host ""
Write-Host "=== WATCH/WAIT (RunTag=$RunTag) ===" -ForegroundColor Cyan
$start = Get-Date
$round = 0

while ($true) {
  $round++
  $remaining = $pending | Where-Object { -not $_.Done }

  # print heartbeat at the start of the round
  if ($StatusEvery -gt 0 -and (($round - 1) % $StatusEvery -eq 0)) {
    Show-StatusTable -Tasks $pending -Round $round -Start $overallStart -Clear:$WatchClear
  }

  if (-not $remaining) { break }

  foreach ($task in $remaining) {
    # If remote run dir missing (e.g., cleaned up outside), mark invalid and advise
    $dirCheck = Invoke-SSH -EnvCfg $task.EnvCfg -RemoteCommand "test -d '$($task.RemoteRunDir)' && echo OK || echo MISS" -HardTimeoutSec $SshHardTimeoutSec
    
    if ($TraceSSH) {
      Write-Host ("[TRACE] polling {0}/{1}" -f $task.EnvName, $task.PlatformName) -ForegroundColor DarkGray
    }

    if ($dirCheck.ExitCode -eq 255 -and $dirCheck.StdErr -match 'HARD TIMEOUT') {
      if ($task.State -ne 'Timeout') { $task.LastStateChange = Get-Date }
      $task.State = 'Timeout'
      continue
    }

    if ($dirCheck.StdOut.Trim() -eq 'MISS') {
      if ($task.State -ne 'Missing') { $task.LastStateChange = Get-Date }
      $task.State = 'Missing'
      $task.Done = $true
      $task.ExitCode = $null
      continue
    }

    $st = Check-PlatformStatus -EnvCfg $task.EnvCfg -RemoteRunDir $task.RemoteRunDir -PlatformName $task.PlatformName

    # update state/last-change
    if ($task.State -ne $st.State -or $task.ExitCode -ne $st.ExitCode) {
      $task.LastStateChange = Get-Date
    }
    $task.State = $st.State
    $task.ExitCode = $st.ExitCode

    switch ($st.State) {
      'Finished' {
        # download outputs then mark done
        $localRoot = [IO.Path]::Combine($OutRoot, [IO.Path]::Combine($task.EnvName, $task.PlatformName))
        Download-PlatformOutputs -EnvCfg $task.EnvCfg -RemoteRunDir $task.RemoteRunDir -PlatformName $task.PlatformName -LocalRoot $localRoot
        $task.Done = $true
        Write-Host ("[{0}] {1}/{2} -> exit {3}" -f (Get-Date).ToString('HH:mm:ss'), $task.EnvName, $task.PlatformName, $st.ExitCode)
      }
      default { }
    }
  }

  if ($StatusEvery -gt 0 -and ($round % $StatusEvery -eq 0)) {
    Show-StatusTable -Tasks $pending -Round $round -Start $overallStart -Clear:$WatchClear
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
    $rrd = $remoteRunDirByEnv[$envCfg.Name]
    if (-not $rrd) {
      $rrd = Get-RemoteRunDir -EnvCfg $envCfg -RunTag $RunTag
    }
    $check = Invoke-SSH -EnvCfg $envCfg -RemoteCommand "test -d '$rrd' && echo OK || echo MISS" -HardTimeoutSec $SshHardTimeoutSec
    if ($check.StdOut.Trim() -eq 'OK') {
      [void](Invoke-SSH -EnvCfg $envCfg -RemoteCommand "rm -rf '$rrd'" -HardTimeoutSec $SshHardTimeoutSec)
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

# Decide exit code: 0 if all Done with exit 0; 1 otherwise (includes Missing/Timeout)
$allDone = ($pending | Where-Object { -not $_.Done }).Count -eq 0
$anyFail = ($pending | Where-Object {
  $_.Done -and (
    ($_.State -eq 'Finished' -and ($_.ExitCode -ne 0 -and $null -ne $_.ExitCode)) -or
    ($_.State -in @('Missing','Timeout'))
  )
}).Count -gt 0

$code = if ($allDone -and -not $anyFail) { 0 } else { 1 }
exit $code
