#Aquí se implementa la lógica para leer las instrucciones en lenguaje SQL

# tinysqldb.ps1

function Execute-MyQuery { #Función encargada de leer cada linea del documento con instrucciones y pasarlas
                            #al server donde se procesarán, también se toma en cuenta 
    param (
        [Parameter(Mandatory=$true)]
        [string]$QueryFile, #path donde se encuentra el archivo con sentencias SQL por ejecutar
        
        [Parameter(Mandatory=$true)]
        [int]$Port, #Puerto en el que API Interface escucha
        
        [Parameter(Mandatory=$true)]
        [string]$IP #Dirección IP donde el API Interface escucha
    )
    Write-Host "Iniciando la ejecución del script tinysqldb.ps1"


    # Leer el contenido del archivo de consulta
    $consultas = Get-Content $QueryFile -Raw

    # Dividir las consultas (asumiendo que están separadas por punto y coma) y las agregamos a una lista.
    $queryList = $consultas -split ';'

    foreach ($query in $queryList) {
        if (-not [string]::IsNullOrWhiteSpace($query)) {
            $startTime = Get-Date #Iniciamos a contar el tiempo desde aquí, pasa por todo el proyecto.
            
            Write-Host "Ejecutando consulta: $query"
            # Aquí iría la lógica para enviar la consulta al servidor
            $result = EnviarConsultas-AlServer -Query $query -IP $IP -Port $Port
	
	        #Luego de que toda la lógica saque el resultado, se muestra en pantalla:
            $endTime = Get-Date #Luego se para de contar el tiempo.
            $duracionTotal = $endTime - $startTime #Finalmente se hace una diferencia entre el tiempo de inicio y el tiempo final para determinar el tiempo total.

            # Mostrar el resultado en formato de tabla
            if ($result -and $result.Count -gt 0) {
                $result | Format-Table
            } else {
                Write-Host "La consulta no devolvió resultados."
            }

            Write-Host "Tiempo de ejecución: $($duracionTotal.TotalMilliseconds) ms"
        }
    }
}


function EnviarConsultas-AlServer {
    param (
        [string]$Query,
        [string]$IP,
        [int]$Port
    )

    # Aquí se implementa la lógica para enviar la consulta al servidor
    # y recibir la respuesta por medio de sockets TCP.
    
    # Por ahora, un ejemplo de una respuesta
    Write-Host "Enviando consulta al servidor ${IP}:$Port"
    
    Write-Host "Ejecutando consulta desde archivo: $Query"
    
    # Simulando la ejecución de la consulta
    switch -regex ($Query) {
        "CREATE DATABASE" { Write-Host "Base de datos creada exitosamente." }
        "CREATE TABLE" { Write-Host "Tabla creada exitosamente." }
        "INSERT INTO" { Write-Host "Registro insertado correctamente." }
        "SELECT \* FROM Users" {
            $result = @(
                @{ ID = 1; Name = "John Doe" }
            )
            return $result
        }
        default { Write-Host "Consulta desconocida o no soportada." }
    }
}


# Convertir el puerto a entero y luego llamar a la función
$port = [int]$args[1]
Execute-MyQuery -QueryFile $args[0] -Port $port -IP $args[2]

# Exportar la función para que esté disponible cuando se importe el módulo(ver si se ocupa esto!!!!)
#Export-ModuleMember -Function Execute-MyQuery