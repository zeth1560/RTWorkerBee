@echo off
REM Stream Deck: "System" -> "Open" -> this file (no arguments field needed).
REM Change the port if yours is not 8791 (must match REPLAY_TRIGGER_HTTP_PORT).
set "RT_REPLAY_PORT=8791"

powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -Command "$ts=[DateTimeOffset]::UtcNow.ToUnixTimeSeconds(); $rid='sd-'+[guid]::NewGuid().ToString('N').Substring(0,8); $u=('http://127.0.0.1:'+$env:RT_REPLAY_PORT+'/replay?trigger='+$ts+'&request_id='+$rid); Invoke-RestMethod -Uri $u -Method Get | Out-Null"
