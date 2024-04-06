[![Big-Data-Hadoop.png](https://i.postimg.cc/Kc668GWV/Big-Data-Hadoop.png)](https://postimg.cc/PNM36jTz)

## Introducción

El proyecto de Importaciones Vehiculares tiene como objetivo realizar análisis de datos sobre las importaciones de vehículos utilizando herramientas de Big Data como Hadoop, Sqoop, Hive y PySpark. Para facilitar la configuración y las instalaciones, se han creado contenedores Docker para cada una de estas herramientas.

## Herramientas Utilizadas

<b>Hadoop:</b><br> Plataforma de código abierto para almacenamiento y procesamiento distribuido de grandes conjuntos de datos.

<b>Sqoop:</b><br> Herramienta para transferir datos entre bases de datos relacionales y Hadoop.

<b>Hive:</b><br> Sistema de almacenamiento y procesamiento de datos basado en Hadoop que proporciona un lenguaje de consulta similar a SQL llamado HiveQL.

<b>PySpark:</b><br> API de Python para Apache Spark, que permite el procesamiento de datos distribuidos en clústeres de computadoras.

<b>Jupyter:</b><br> Entorno de desarrollo interactivo utilizando Jupyter Notebook. Permite ejecutar código Python, visualizar datos y escribir documentación en un formato de cuaderno interactivo

<b>Spark Master:</b><br> Aloja el nodo maestro de Spark, que coordina las tareas en un clúster Spark. Proporciona acceso a la interfaz web del maestro de Spark y el puerto de comunicación entre los trabajadores y el maestro.

<b>Spark Worker:</b><br> Aloja un nodo trabajador de Spark, que ejecuta tareas de procesamiento de datos. Dependiendo del servicio spark-master, proporciona acceso a la interfaz web del trabajador de Spark.

## Estructura del Proyecto

BigData_Haddop/<br>
│<br>
├── datanode/<br>
│   ├── jdbc/<br>
│   ├── scripts/<br>
│   ├── sqoop/<br>
│   └── Dockerfile<br>
│<br>
├── mysql/<br>
│   └── Dockerfile<br>
│   └── importaciones_db.sql<br>
│   └── variables.env<br>
│<br>
├── python/<br>
│   ├── source/<br>
│   ├── source/<br>
│   └── Dockerfile<br>
│<br>
└── README.md<br>
└── docker-compose.yaml<br>
└── hadoop-hive.env<br>

## Instrucciones de Uso

Para utilizar este proyecto, sigue estos pasos:

1. Clona este repositorio en tu máquina local:

		git clone https://github.com/jcarlosmamanidelacruz/BigData_Hadoop

2. Instala Docker si no lo tienes instalado. Puedes seguir las instrucciones de instalación en la documentación oficial de Docker.

3. Abre una terminal y navega al directorio del proyecto:

		cd BigData_Haddop

4. Ejecuta el siguiente comando para iniciar los contenedores:

		docker-compose up
Este comando iniciará todos los servicios definidos en el archivo docker-compose.yaml.

## Configuración de Visual Studio Code

Para facilitar la gestión de los contenedores Docker y la interacción con la base de datos MySQL, se recomienda instalar las siguientes extensiones en Visual Studio Code:

1. Docker Explorer: Esta extensión proporciona una interfaz gráfica dentro de Visual Studio Code para administrar contenedores Docker, ver imágenes, redes y volúmenes, entre otras funciones. Para instalarla, sigue estos pasos:

	- Abre Visual Studio Code.
	- Ve al menú de extensiones (Ctrl+Shift+X o Cmd+Shift+X en macOS).
	- Busca "Docker Explorer".
	- Haz clic en "Instalar".

2. MySQL v7.1.8: Esta extensión permite conectarse a una base de datos MySQL desde Visual Studio Code y ejecutar consultas SQL directamente en el editor. Para instalarla, sigue estos pasos:

	- Abre Visual Studio Code.
	- Ve al menú de extensiones (Ctrl+Shift+X o Cmd+Shift+X en macOS).
	- Busca "MySQL".
	- Haz clic en "Instalar".

3. Desplegar container

	1. Crear un codespace para el repositorio e ingresar al mismo
	2. Abrir terminal de codespace
	3. Ejecutar el siguiente comando para desplegar los contenedores
			>_ docker-compose up
Esta linea desplegara los contenedores y podras ver estos utilizando la extension Docker explorer

[![docker-comse-up.png](https://i.postimg.cc/vZh0M1R8/docker-comse-up.png)](https://postimg.cc/XrZfkY7t)
## MySQL

Este contenedor contiene una base de datos llamada importaciones_db y consta de las tablas que se verá en al imagen.

Las credenciales para conectarnos a nuestra base de datos es misma que encontrar en el archivo: mysql/Dockerfile :

user: root
pass: root
port: 3310

Ejecutar ifconfig en terminal para obtener la ip (eth0)

[![Mysql.png](https://i.postimg.cc/ZY24sy9N/Mysql.png)](https://postimg.cc/yWmMkxy6)
## Hadoop

Para poder trabajar con <b>hadoop</b> ingresamos al contenedor del datanode.

Abrimos un terminal nuevo y ejecutamos lo siguiente:

		docker exec -it datanode bash

Asi para cada contenedor con el que queremos trabajar.

Para utilizar <b>sqoop</b> en el datanode debemos ejecutar lo siguiente:

		sh /datanode/scripts/script.sh

[![utilizar-sqoop.png](https://i.postimg.cc/sgQ8y5YP/utilizar-sqoop.png)](https://postimg.cc/9D21YRNz)

Para exportar las tablas de la base de datos importaciones_db con <b>sqoop al HDFS de hadoop </b>ejecutar lo siguiente: 

		sh /datanode/scripts/sqoop/script_sqoop_textfile_importaciones.sh

[![script-sqoop.png](https://i.postimg.cc/jSKkGCxv/script-sqoop.png)](https://postimg.cc/MvPDB6jj)

Para visualizar los ficheros en el HDFS de hadoop:

[![HDFS-SQOOP.png](https://i.postimg.cc/9fXT5Hwv/HDFS-SQOOP.png)](https://postimg.cc/XrhXdh88)

## Hive

Para poder trabajar con hive ingresamos al contenedor del hive-server.

Abrir un terminal y copiar el archivo hive.hql a hive-server

		docker cp datanode/scripts/hive/hive_importaciones_db.hql hive-server:/opt

Abrimos un terminal nuevo y ejecutamos lo siguiente

		docker exec -it hive-server bash

Para crear tablas externas en base a los datos importados con sqoop ejecutamos los siguientes pasos:

En el terminal de hive-server ejecutamos lo siguiente para crear las tablas

		hive -f /opt/hive.hql

[![tablas-hive.png](https://i.postimg.cc/pXVPcxjh/tablas-hive.png)](https://postimg.cc/kDZrD0fq)

En el terminal de hive-server ejecutamos lo siguiente para listar las base de datos de hive

		hive

En el terminal de hive-server  luego de haber ejecutado el comando hive ejecutamos lo siguiente

		show databases;
		use importaciones_db;
		show tables;

[![comandos-hive.png](https://i.postimg.cc/W4pknxns/comandos-hive.png)](https://postimg.cc/TKS127Fs)

En la misma terminal de hive-server ejecutamos lo siguiente comando para verificar el contenido de las tablas en hive;

		select * from aduana_ingreso;

[![tablas-hive-select.png](https://i.postimg.cc/Fzn4MktP/tablas-hive-select.png)](https://postimg.cc/sGSqSXgS)

## Conexión a Hive en Hadoop utilizando Spark

Para trabajar con las tablas migradas de `importaciones_db` en Hadoop, utilizaremos Spark. A continuación, se detallan los pasos para conectarte a Hive y realizar análisis de datos utilizando PySpark desde Jupyter Notebook:

Una vez que los contenedores estén en funcionamiento, puedes utilizar Docker Explorer en Visual Studio Code para acceder a los puertos expuestos por los contenedores acá ubicaremos el puerto 8200 que es el puerto expuesto para nuestro Jupyter Notebook

[![puerto-expuestos.png](https://i.postimg.cc/nzsmN5PB/puerto-expuestos.png)](https://postimg.cc/8JSs6yDz)

Una vez que hayas abierto Jupyter Notebook utilizando Docker Explorer, encontrarás un archivo <b>SparkHive-checkpoint.ipynb.</b> Para acceder al archivo que contiene los códigos para conectarse a Hadoop y Hive, así como las queries utilizadas para el análisis de datos.

[![jupyter-query1.png](https://i.postimg.cc/zfMf9fWX/jupyter-query1.png)](https://postimg.cc/p9fxjRqg)

[![jupyter-query2.png](https://i.postimg.cc/4NJX2cMQ/jupyter-query2.png)](https://postimg.cc/3WcMdk3k)

