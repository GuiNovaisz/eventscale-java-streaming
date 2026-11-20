param([string]$BaseUrl='http://localhost:8080')
$ErrorActionPreference='Stop'
$order='smoke-'+[guid]::NewGuid().ToString('N')
$ids=@()
foreach ($type in @('order.created','payment.confirmed','order.shipped','order.delivered')) {
    $id=[guid]::NewGuid().ToString(); $ids+=$id
    $body=@{eventId=$id;eventType=$type;eventVersion=1;aggregateId=$order;occurredAt='2026-11-01T12:00:00Z';producer='smoke';correlationId=$order;payload=@{orderId=$order;customerId='demo';total=42.50;currency='BRL'}} | ConvertTo-Json -Depth 4 -Compress
    $result=Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/events" -ContentType 'application/json' -Body $body
    if($result.status -notin @('ACCEPTED','PUBLISHED')) { throw 'Unexpected intake result' }
}
$deadline=[datetime]::UtcNow.AddSeconds(90)
do {
    try { $state=Invoke-RestMethod "$BaseUrl/api/v1/orders/$order" } catch { $state=$null }
    if($state.status -eq 'DELIVERED') { break }
    Start-Sleep -Milliseconds 200
} while([datetime]::UtcNow -lt $deadline)
if($state.status -ne 'DELIVERED') { throw 'Projection did not catch up' }
$receipts=@($ids | ForEach-Object { Invoke-RestMethod "$BaseUrl/api/v1/events/$_" })
if(@($receipts | Where-Object { !$_.published_at }).Count -ne 0) { throw 'Kafka publication unconfirmed' }
if(@($receipts.partition_id | Select-Object -Unique).Count -ne 1) { throw 'Same key crossed partitions' }
$analytics=Invoke-RestMethod "$BaseUrl/api/v1/analytics"
Write-Output (@{result='PASS';order=$order;status=$state.status;partition=$receipts[0].partition_id;offsets=$receipts.kafka_offset;analytics=$analytics} | ConvertTo-Json -Depth 6)
