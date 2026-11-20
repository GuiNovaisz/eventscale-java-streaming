param([Parameter(Mandatory)][ValidatePattern('^[a-zA-Z0-9_-]{1,48}$')][string]$Version)
$ErrorActionPreference='Stop'
if($Version -eq 'v1') { throw 'Use a NEW projection version and consumer groups.' }
docker compose run -d --no-deps --name "eventscale-replay-$Version" -e "PROJECTION_VERSION=$Version" -e NORMALIZER_ENABLED=false -e EVENTSCALE_DISPATCH_ENABLED=false -e SPRING_MAIN_WEB_APPLICATION_TYPE=none app
if($LASTEXITCODE -ne 0) { throw 'Replay process failed to start' }
Write-Output "Building $Version with new groups. Inspect group lag and compare projections before explicit activation; see docs/replay.md."
