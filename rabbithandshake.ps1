$client = New-Object Net.Sockets.TcpClient("10.31.220.52",5672)
$stream = $client.GetStream()
$stream.Write([byte[]](65,77,81,80,0,0,9,1),0,8)   # "AMQP\0\0\9\1"
$buf = New-Object byte[] 64
$read = $stream.Read($buf,0,64)
$buf[0..($read-1)] | ForEach-Object { "{0:X2}" -f $_ }
$client.Close()

