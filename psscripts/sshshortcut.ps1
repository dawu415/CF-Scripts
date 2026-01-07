# ===========================
# SSH/SCP helpers + shortcut registry
# ===========================

# In-session registry of shortcuts (name -> connection info)
if (-not $script:SshShortcutRegistry) {
  $script:SshShortcutRegistry = [System.Collections.Specialized.OrderedDictionary]::new()
}

function Get-SshShortcuts {
  [CmdletBinding()]
  param(
    [switch]$Detailed
  )

  if ($script:SshShortcutRegistry.Count -eq 0) {
    Write-Host "No SSH shortcuts registered in this session."
    return
  }

  if (-not $Detailed) {
    $script:SshShortcutRegistry.Keys | Sort-Object | ForEach-Object { $_ }
    return
  }

  $script:SshShortcutRegistry.Keys | Sort-Object | ForEach-Object {
    $v = $script:SshShortcutRegistry[$_]
    [pscustomobject]@{
      Name     = $_
      Host     = $v.SSHHost
      User     = $v.User
      Port     = $v.Port
      KeyPath  = $v.KeyPath
      Password = [bool]$v.UsePassword
      Extra    = ($v.ExtraArgs -join ' ')
    }
  } | Format-Table -AutoSize
}

function ezcmd {
  [CmdletBinding()]
  param([switch]$Detailed)

  if ($script:SshShortcutRegistry.Count -eq 0) {
    Write-Host "No SSH shortcuts registered in this session."
    return
  }

  $names = $script:SshShortcutRegistry.Keys | Sort-Object

  if (-not $Detailed) {
    Write-Host "SSH shortcuts:"
    $names | ForEach-Object { "  $_" } | Write-Host

    Write-Host ""
    Write-Host "SCP shortcuts:"
    $names | ForEach-Object { "  ${_}_scp" } | Write-Host

    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  ABC ls -la"
    Write-Host "  ABC_scp C:\tmp\a.txt /home/svc/a.txt"
    Write-Host "  ABC_scp /var/log/syslog C:\tmp\syslog"
    return
  }

  $names | ForEach-Object {
    $v = $script:SshShortcutRegistry[$_]
    [pscustomobject]@{
      Name    = $_
      SshCmd  = $_
      ScpCmd  = "${_}_scp"
      Host    = $v.SSHHost
      User    = $v.User
      Port    = $v.Port
      KeyPath = $v.KeyPath
      Extra   = ($v.ExtraArgs -join ' ')
    }
  } | Format-Table -AutoSize
}

Set-Alias -Name ezcmds -Value ezcmd -ErrorAction SilentlyContinue


function Invoke-SshEz {
  [CmdletBinding(PositionalBinding=$true)]
  param(
    [Parameter(Mandatory=$true)] [string]$SSHHost,
    [Parameter()] [string]$User,
    [Parameter()] [int]$Port = 22,
    [Parameter()] [string]$KeyPath,
    [Parameter()] [switch]$UsePassword,
    [Parameter()] [switch]$NoStrictHostKeyChecking = $true,
    [Parameter()] [switch]$NoKnownHosts = $true,
    [Parameter()] [string]$Cipher = 'aes128-ctr',
    [Parameter()] [string]$MACs   = 'hmac-sha2-256',
    [Parameter()] [string[]]$ExtraArgs,

    # Keep your explicit RemoteCommand option
    [Parameter()] [Object]$RemoteCommand,

    # New: passthrough (like ssh ... <command...>)
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$CommandArgs
  )

  $ssh = (Get-Command ssh -ErrorAction SilentlyContinue)?.Source
  if (-not $ssh) { throw "OpenSSH 'ssh' not found on PATH. Install the OpenSSH client." }

  # If RemoteCommand not provided, use trailing args as the command
  if ($null -eq $RemoteCommand -and $CommandArgs -and $CommandArgs.Count -gt 0) {
    $RemoteCommand = $CommandArgs
  }

  $nullKH = if ($IsWindows) { 'NUL' } else { '/dev/null' }
  $sshargs = @()

  if ($Port -ne 22) { $sshargs += @('-p', $Port) }

  if ($KeyPath) {
    $expandedKey = $KeyPath -replace '^~', $HOME
    $sshargs += @('-i', $expandedKey)
  } elseif ($UsePassword) {
    $sshargs += @('-o','PreferredAuthentications=password', '-o','PubkeyAuthentication=no')
  }

  if ($NoStrictHostKeyChecking) { $sshargs += @('-o','StrictHostKeyChecking=no') }
  if ($NoKnownHosts)            { $sshargs += @('-o',"UserKnownHostsFile=$nullKH") }
  if ($Cipher) { $sshargs += @('-o', "Ciphers=$Cipher") }
  if ($MACs)   { $sshargs += @('-o', "MACs=$MACs") }
  if ($ExtraArgs) { $sshargs += $ExtraArgs }

  $target = if ($User) { "$User@$SSHHost" } else { $SSHHost }
  $sshargs += $target

  if ($null -ne $RemoteCommand) {
    if ($RemoteCommand -is [Array]) { $sshargs += $RemoteCommand } else { $sshargs += @("$RemoteCommand") }
  }

  & $ssh @sshargs
}

function Invoke-ScpEz {
  [CmdletBinding(PositionalBinding=$true)]
  param(
    # --- Connection bits (same as before) ---
    [Parameter(Mandatory=$true)] [string]$SSHHost,
    [Parameter()] [string]$User,
    [Parameter()] [int]$Port = 22,
    [Parameter()] [string]$KeyPath,
    [Parameter()] [switch]$UsePassword,
    [Parameter()] [switch]$NoStrictHostKeyChecking = $true,
    [Parameter()] [switch]$NoKnownHosts = $true,
    [Parameter()] [string]$Cipher = 'aes128-ctr',
    [Parameter()] [string]$MACs   = 'hmac-sha2-256',
    [Parameter()] [string[]]$ExtraArgs,
    [Parameter()] [switch]$Recursive,
    [Parameter()] [switch]$PreserveTimes,

    # --- Optional named style ---
    [Parameter()] [string[]]$Source,
    [Parameter()] [string]$Destination,

    # --- Positional passthrough style ---
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$PathArgs
  )

  $scp = (Get-Command scp -ErrorAction SilentlyContinue)?.Source
  if (-not $scp) { throw "OpenSSH 'scp' not found on PATH. Install the OpenSSH client." }

  # If Source/Destination not provided, infer from remaining args.
  if ((-not $Source -or $Source.Count -eq 0) -and -not $Destination) {
    if (-not $PathArgs -or $PathArgs.Count -lt 2) {
      throw "Provide either -Source/-Destination OR positional args: <src1> [src2 ...] <dest>."
    }
    $Destination = $PathArgs[-1]
    $Source = $PathArgs[0..($PathArgs.Count - 2)]
  } elseif (($Source -and -not $Destination) -or (-not $Source -and $Destination)) {
    throw "If you use -Source or -Destination, you must provide both."
  }

  $nullKH = if ($IsWindows) { 'NUL' } else { '/dev/null' }
  $scpargs = @()
  if ($Recursive)     { $scpargs += '-r' }
  if ($PreserveTimes) { $scpargs += '-p' }
  if ($Port -ne 22)   { $scpargs += @('-P', $Port) }

  if ($KeyPath) {
    $expandedKey = $KeyPath -replace '^~', $HOME
    $scpargs += @('-i', $expandedKey)
  } elseif ($UsePassword) {
    $scpargs += @('-o','PreferredAuthentications=password', '-o','PubkeyAuthentication=no')
  }

  if ($NoStrictHostKeyChecking) { $scpargs += @('-o','StrictHostKeyChecking=no') }
  if ($NoKnownHosts)            { $scpargs += @('-o',"UserKnownHostsFile=$nullKH") }
  if ($Cipher) { $scpargs += @('-o', "Ciphers=$Cipher") }
  if ($MACs)   { $scpargs += @('-o', "MACs=$MACs") }
  if ($ExtraArgs) { $scpargs += $ExtraArgs }

  $userAtHost = if ($User) { "$User@$SSHHost" } else { $SSHHost }

  # Convenience: allow ":/path" as "remote on this shortcut host"
  $srcExpanded = $Source | ForEach-Object { if ($_ -match '^:') { "$userAtHost$_" } else { $_ } }
  $dstExpanded = if ($Destination -match '^:') { "$userAtHost$Destination" } else { $Destination }

  & $scp @scpargs @srcExpanded $dstExpanded
}


function New-SshShortcut {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$true)] [ValidatePattern('^[A-Za-z_][A-Za-z0-9_-]*$')] [string]$Name,
    [Parameter(Mandatory=$true)] [string]$SSHHost,
    [Parameter()] [string]$User,
    [Parameter()] [int]$Port = 22,
    [Parameter()] [string]$KeyPath,
    [Parameter()] [switch]$UsePassword,
    [Parameter()] [switch]$NoStrictHostKeyChecking = $true,
    [Parameter()] [switch]$NoKnownHosts = $true,
    [Parameter()] [string]$Cipher = 'aes128-ctr',
    [Parameter()] [string]$MACs   = 'hmac-sha2-256',
    [Parameter()] [string[]]$ExtraArgs
  )

  # Register in-memory for listing later
  $script:SshShortcutRegistry[$Name] = @{
    SSHHost   = $SSHHost
    User      = $User
    Port      = $Port
    KeyPath   = $KeyPath
    UsePassword = [bool]$UsePassword
    NoStrictHostKeyChecking = [bool]$NoStrictHostKeyChecking
    NoKnownHosts = [bool]$NoKnownHosts
    Cipher    = $Cipher
    MACs      = $MACs
    ExtraArgs = $ExtraArgs
  }

  # Build the call parts used by generated functions
  $parts = @()
  $parts += "-SSHHost '$SSHHost'"
  if ($User) { $parts += "-User '$User'" }
  if ($Port -ne 22) { $parts += "-Port $Port" }
  if ($KeyPath) { $safeKey = $KeyPath -replace "'", "''"; $parts += "-KeyPath '$safeKey'" }
  if ($UsePassword) { $parts += "-UsePassword" }
  if ($NoStrictHostKeyChecking) { $parts += "-NoStrictHostKeyChecking" } else { $parts += "-NoStrictHostKeyChecking:`$false" }
  if ($NoKnownHosts) { $parts += "-NoKnownHosts" } else { $parts += "-NoKnownHosts:`$false" }
  if ($Cipher) { $parts += "-Cipher '$Cipher'" }
  if ($MACs)   { $parts += "-MACs '$MACs'" }
  if ($ExtraArgs) {
    $quoted = $ExtraArgs | ForEach-Object { "'$($_ -replace "'", "''")'" }
    $parts += "-ExtraArgs @($($quoted -join ', '))"
  }

  # Generate:
  #   <Name>        -> ssh shortcut
  #   <Name>_scp    -> scp shortcut
  $body = @"
function global:$Name {
  [CmdletBinding(PositionalBinding=`$true)]
  param(
    [Parameter()] [Object]`$RemoteCommand,
    [Parameter()] [string[]]`$ExtraArgs,

    [Parameter(ValueFromRemainingArguments=`$true)]
    [string[]]`$CommandArgs
  )

  if (`$null -eq `$RemoteCommand -and `$CommandArgs -and `$CommandArgs.Count -gt 0) {
    `$RemoteCommand = `$CommandArgs
  }

  if (`$PSBoundParameters.ContainsKey('ExtraArgs')) {
    Invoke-SshEz $($parts -join ' ') -ExtraArgs `$ExtraArgs -RemoteCommand `$RemoteCommand
  } else {
    Invoke-SshEz $($parts -join ' ') -RemoteCommand `$RemoteCommand
  }
}

function global:${Name}_scp {
  [CmdletBinding(PositionalBinding=`$true)]
  param(
    [Parameter()] [string[]]`$Source,
    [Parameter()] [string]`$Destination,
    [Parameter()] [switch]`$Recursive,
    [Parameter()] [switch]`$PreserveTimes,
    [Parameter()] [string[]]`$ExtraArgs,

    [Parameter(ValueFromRemainingArguments=`$true)]
    [string[]]`$PathArgs
  )

  # If caller didn't use -Source/-Destination, pass positional args through.
  if ((-not `$Source -or `$Source.Count -eq 0) -and -not `$Destination) {
    if (`$PSBoundParameters.ContainsKey('ExtraArgs')) {
      Invoke-ScpEz $($parts -join ' ') -ExtraArgs `$ExtraArgs -Recursive:`$Recursive -PreserveTimes:`$PreserveTimes @`$PathArgs
    } else {
      Invoke-ScpEz $($parts -join ' ') -Recursive:`$Recursive -PreserveTimes:`$PreserveTimes @`$PathArgs
    }
    return
  }

  # Named style
  if (`$PSBoundParameters.ContainsKey('ExtraArgs')) {
    Invoke-ScpEz -Source `$Source -Destination `$Destination $($parts -join ' ') -ExtraArgs `$ExtraArgs -Recursive:`$Recursive -PreserveTimes:`$PreserveTimes
  } else {
    Invoke-ScpEz -Source `$Source -Destination `$Destination $($parts -join ' ') -Recursive:`$Recursive -PreserveTimes:`$PreserveTimes
  }
}
"@

  Remove-Item "function:\$Name" -ErrorAction SilentlyContinue
  Remove-Item "function:\${Name}_scp" -ErrorAction SilentlyContinue
  Invoke-Expression $body
}

Set-Alias -Name sshx -Value Invoke-SshEz -ErrorAction SilentlyContinue
Set-Alias -Name scpx -Value Invoke-ScpEz -ErrorAction SilentlyContinue
Set-Alias -Name sshshort -Value Get-SshShortcuts -ErrorAction SilentlyContinue


New-SshShortcut -Name FoundationA -User A -SSHHost A.com -Port 22 -KeyPath C:\Users\xxx\.ssh\id_rsa -ExtraArgs @('-o','ServerAliveInterval=30')
New-SshShortcut -Name FoundationB -User B -SSHHost B.com -Port 22 -KeyPath C:\Users\xxx\.ssh\id_ed25519 -ExtraArgs @('-o','ServerAliveInterval=30')


# Friendly tip on load
Write-Host "SSH shortcuts loaded: $($SshShortcuts.Name -join ', ')" -ForegroundColor Cyan
Write-Host "Try: web1  or  web1 -RemoteCommand 'hostname'" -ForegroundColor DarkCyan
