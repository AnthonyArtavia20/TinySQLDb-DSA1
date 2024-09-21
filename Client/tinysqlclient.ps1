#Parámetros que recibe el programa PowerShell
param (
    [Parameter(Mandatory = $true)]
    [string]$IP,
    [Parameter(Mandatory = $true)]
    [int]$Port
)

#Obtenemos la IP Address y puerto
$ipAddress = if ($IP -eq "localhost") { [System.Net.IPAddress]::Loopback } else { [System.Net.IPAddress]::Parse($IP) }
$ipEndPoint = [System.Net.IPEndPoint]::new($ipAddress, $Port)

function Send-Message { #Función encargada de enviar las instrucciones
    param (
        [Parameter(Mandatory=$true)]
        [string]$message,
        [Parameter(Mandatory=$true)]
        [System.Net.Sockets.Socket]$client
    )

    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $writer = New-Object System.IO.StreamWriter($stream)
    try {
        $writer.WriteLine($message)
        $writer.Flush()
    }
    finally {
        $writer.Close()
        $stream.Close()
    }
}

function Receive-Message { #Función Encargada de recibir el mensaje enviado desde el ApiInterface
    param (
        [System.Net.Sockets.Socket]$client
    )
    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $reader = New-Object System.IO.StreamReader($stream)
    try {
        return $reader.ReadLine()
    }
    finally {
        $reader.Close()
        $stream.Close()
    }
}

function Send-SQLCommand {#Función encargada de poder enviar las consultas y utilizar los métodos anteriores
    param (
        [string]$command
    )
    try {
        Write-Host "Attempting to connect to $($ipEndPoint.Address):$($ipEndPoint.Port)"
        $client = New-Object System.Net.Sockets.Socket($ipEndPoint.AddressFamily, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)
        $client.Connect($ipEndPoint)
        Write-Host "Connected successfully"
        
        $requestObject = [PSCustomObject]@{
            RequestType = 0;
            RequestBody = $command
        }
        Write-Host -ForegroundColor Green "Sending command: $command"
        
        $jsonMessage = ConvertTo-Json -InputObject $requestObject -Compress
        Send-Message -client $client -message $jsonMessage
        $response = Receive-Message -client $client
        
        Write-Host -ForegroundColor Green "Response received: $response"
        
        if ($response) {
            $responseObject = ConvertFrom-Json -InputObject $response
            return $responseObject
        } else {
            Write-Host -ForegroundColor Yellow "No response received from server."
            return $null
        }
    }
    catch {
        Write-Host -ForegroundColor Red "Error: $_"
        Write-Host -ForegroundColor Red "StackTrace: $($_.ScriptStackTrace)"
        return $null
    }
    finally {
        if ($client -and $client.Connected) {
            $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
            $client.Close()
        }
    }
}

function Start-MyQuery { #Función principal, donde se leen las consultas y llama a los métodos correctos
    #para poder enviar, recibir, y mostrar los resultas de las operaciones.

    # Obtener el archivo de consulta en el mismo directorio que el script
    $queryFilePath = Join-Path $PSScriptRoot "ScriptDeEjemplo.tinysql"
    
    Write-Host "Iniciando la ejecución del script tinysqldb.ps1"
    Write-Host "Leyendo el archivo de consulta en: $queryFilePath"
    
    # Leer el contenido del archivo de consulta
    $consultas = Get-Content $queryFilePath -Raw
    $queryList = $consultas -split ';'
    
    foreach ($query in $queryList) {
        $query = $query.Trim()
        if (-not [string]::IsNullOrWhiteSpace($query)) {
            $startTime = Get-Date
        
            Write-Host "Ejecutando consulta: $query"
            $result = Send-SQLCommand -command $query

            $endTime = Get-Date
            $duracionTotal = $endTime - $startTime
            
            if ($result -and $result.Status -eq 0) {
                if (-not [string]::IsNullOrWhiteSpace($result.ResponseBody)) {
                    $responseData = $result.ResponseBody | ConvertFrom-Json
                    if ($responseData.Data -eq "La tabla está vacía.") {
                        Write-Host $responseData.Data
                    } elseif ($responseData.Data) {
                        $tableData = $responseData.Data -replace '\\r\\n',"`n" | ConvertFrom-Csv #Se añadió esto para poder convertir de Csv que es el formato en que viene la información de la creación de las tablas.
                        if ($tableData) {
                            $tableData | Format-Table
                        } else {
                            Write-Host "La tabla está vacía (solo contiene encabezados)."
                        }
                    } else {
                        Write-Host "La consulta se ejecutó correctamente, pero no devolvió resultados."
                    }
                } else {
                    Write-Host "La consulta se ejecutó correctamente, pero la respuesta está vacía."
                }
            } else {
                Write-Host -ForegroundColor Red "Error al ejecutar la consulta: $($result.ResponseBody)"
            }
            Write-Host "Tiempo de ejecución: $($duracionTotal.TotalMilliseconds) ms"
        }
    }
}

# Llamar a la función para ejecutar las consultas
Start-MyQuery
