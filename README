## TinySQLDb

### ¿Cómo usar?

##### Se  descargan los archivos aquí disponibles y posterior se abre la carpeta en VisualStudioCode, luego desde la terminal, ya sea desde powershell, cmd o bien desde la misma terminal powershell de visual ejecutar la siguiente serie de comandos:

##### 1) Iniciar el servidor:
 Se realiza la navegación hasta la carpeta "ApiInterface"por medio del comando:

`PS C:\Users\Usuario\Desktop\TinySQLDb-DSA1> cd ApiInterface`

Posterior se ejecutar el siguiente comando desde la terminal estando dentro del directorio:

Luego el programa iniciará el servidor, posterior se deberá mostrar el mensaje `Server ready at 0.0.0.0:11000` además si anteriormente no se creó un índice, el servidor deberá mostrar: 
`No se encontró el archivo SystemIndexes.Indexes. No hay índices para recrear.`

`PS c:\users\Usuario\Desktop\> donet run` Posterior se iniciará el juego.

##### 2) Hacer consultas en el programa como cliente:
Similar a iniciar el servidor, se deberá navegar a la carpeta Client de la siguiente manera: `PS C:\Users\Usuario\Desktop\TinySQLDb-DSA1> cd Client`

Posteriormente se ejecuta el siguiente comando:
`. .\tinysqlclient.ps1 -IP "localhost" -Port 11000`, esto permitirá iniciar el servidor en el puerto 11000 del protocolo TCP/UDP conectándose al servidor.

Luego se leen las operaciones escritas en el documento `"ConsultasYOperaciones.tinysql"` situado en la carpeta Client. Dichas operaciones pueden ser modificadas ahí.

## ¿Cómo funciona este programa?
##### Este programa está hecho en el lenguaje de programación c# utilizando como orientación del funcionamiento interno ejemplos de un gestor de bases de índole comercial. Se implementan distintas estucturas de datos, como las jerárquicas; BST(BinarySearchTree) y BTREE(Balanced Tree) además un algoritmo de ordenamineto "QuickSort"



Este programa plica los conceptos fundamentales de la programación orientada a objetos (POO) y estructuras de jerárquicas (BST y BTREE).

El proyecto consiste en diseñar e implementar un motor de bases de datos relacional relativamente sencillo, con el objetivo de familiarizarse con el funcionamiento de este.
Además se aplican buenas prácticas de programación como lo es la separación de responsabilidades y un patrón de desarrollo modular.

##### Operaciones soportadas:
+ CREATE DATABASE <database-name>;
+ SET DATABASE <database-name>;
+ CREATE TABLE <table-name>;
+ DROP TABLE <table-name>;
+  CREATE INDEX < index-name></index-name> ON < table-name>(column-name) OF TYPE BST/BTREE;
+  SELECT * < Columns> FROM < table-name> WHERE < Column-name>compare-operator [< value>];
		compare-operator: >, <, =
+ UPDATE < table-name>;
+ DELETE FROM < table-name>[WHERE where-statement];
+ INSERT INTO < table-name> (columnas,separadas,por,comas) VALUES (valores,bajo,columnas);