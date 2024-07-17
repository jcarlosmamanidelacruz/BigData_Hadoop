<img src="https://i.postimg.cc/MZ3vD71Z/Big-Data-Hadoop.png" alt="">

# Descripción general del proyecto

El proyecto de Importaciones Vehiculares tiene como objetivo analizar datos sobre las importaciones de vehículos, utilizando un conjunto de herramientas de Big Data para manejar y procesar grandes volúmenes de información. La fuente de datos proviene de registros oficiales de importaciones vehiculares, proporcionando detalles como el tipo de vehículo, el país de origen, la fecha de importación, y otros datos relevantes

### Justificación del Uso del Ecosistema Hadoop

Para este proyecto, se ha elegido el ecosistema de Hadoop debido a sus capacidades robustas y escalables para el almacenamiento y procesamiento de grandes volúmenes de datos. A continuación, se describe cómo cada componente de este ecosistema contribuye a la solución:

**Hadoop:** Proporciona una plataforma de código abierto para el almacenamiento distribuido (HDFS) y el procesamiento de datos. Su arquitectura permite manejar y procesar petabytes de datos de manera eficiente.

**Sqoop:** Facilita la transferencia de datos entre bases de datos relacionales y Hadoop, permitiendo migrar los datos de importaciones vehiculares desde MySQL a HDFS.

**Hive:** Proporciona una capa de almacenamiento y procesamiento de datos sobre Hadoop, utilizando un lenguaje de consulta similar a SQL (HiveQL). Esto permite ejecutar consultas complejas y analizar grandes volúmenes de datos almacenados en HDFS.

**PySpark:** Es la API de Python para Apache Spark, que permite el procesamiento de datos en paralelo en clústeres de computadoras. Spark se utiliza para realizar análisis avanzados y transformaciones de datos según las necesidades del negocio.

**Spark Master:** Aloja el nodo maestro de Spark, que coordina las tareas en un clúster Spark. Proporciona acceso a la interfaz web del maestro de Spark y el puerto de comunicación entre los trabajadores y el maestro.

**Spark Worker:** Aloja un nodo trabajador de Spark, que ejecuta tareas de procesamiento de datos. Dependiendo del servicio Spark Master, proporciona acceso a la interfaz web del trabajador de Spark.

**Docker:** Todo el desarrollo de la infraestructura se realiza utilizando Docker, lo que permite una configuración y despliegue consistentes y replicables. Cada herramienta del ecosistema Hadoop está contenida en su propio contenedor Docker, lo que facilita la gestión y el mantenimiento del entorno de desarrollo.

# Proceso General del Proyecto

A continuación, se describe el proceso general del proyecto, proporcionando una visión macro de las actividades realizadas:

1.- Base de Datos Relacional en MySQL:

- Se inicia con una base de datos MySQL que contiene todas las estructuras OLTP necesarias y los datos poblados de importaciones vehiculares.

2.- Migración de Datos con Sqoop:

- Utilizando Sqoop, se migra la información desde la base de datos MySQL al almacenamiento distribuido HDFS de Hadoop. Este paso asegura que los datos estén disponibles en un sistema de almacenamiento diseñado para manejar grandes volúmenes de datos.

3.- Consultas y Almacenamiento en Hive:

- Hive se utiliza para crear tablas externas basadas en los datos importados con Sqoop. Esto permite realizar consultas SQL sobre los datos almacenados en HDFS, facilitando el análisis y la extracción de información relevante.

4.- Procesamiento de Datos con Spark:

- PySpark se emplea para el procesamiento y análisis de datos. Con Spark, se pueden ejecutar transformaciones complejas y análisis en paralelo, lo que acelera el tiempo de procesamiento y permite manejar grandes volúmenes de datos de manera eficiente.

- Spark Master coordina las tareas en el clúster de Spark, gestionando la distribución del trabajo entre los nodos.

- Spark Worker ejecuta las tareas de procesamiento de datos asignadas por el Spark Master, asegurando que el trabajo se realice de manera distribuida y eficiente.

<img src="https://i.postimg.cc/jS7cKdRN/Data-Haddop.png" alt="">

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

<img src="https://i.postimg.cc/Zn6fHb06/docker-comse-up.png" alt="">

## MySQL

Este contenedor contiene una base de datos llamada importaciones_db y consta de las tablas que se verá en al imagen.

Las credenciales para conectarnos a nuestra base de datos es misma que encontrar en el archivo: mysql/Dockerfile :

user: root
pass: root
port: 3310

Ejecutar ifconfig en terminal para obtener la ip (eth0)

<img src="https://i.postimg.cc/htvLP4gM/Mysql.png" alt="">

## Hadoop

Para poder trabajar con <b>hadoop</b> ingresamos al contenedor del datanode.

Abrimos un terminal nuevo y ejecutamos lo siguiente:

		docker exec -it datanode bash

Asi para cada contenedor con el que queremos trabajar.

Para utilizar <b>sqoop</b> en el datanode debemos ejecutar lo siguiente:

		sh /datanode/scripts/script.sh

<img src="https://i.postimg.cc/sgQ8y5YP/utilizar-sqoop.png" alt="">

Para exportar las tablas de la base de datos importaciones_db con <b>sqoop al HDFS de hadoop </b>ejecutar lo siguiente: 

		sh /datanode/scripts/sqoop/script_sqoop_textfile_importaciones.sh

<img src="https://i.postimg.cc/jSKkGCxv/script-sqoop.png" alt="">

Para visualizar los ficheros en el HDFS de hadoop:

<img src="https://i.postimg.cc/9fXT5Hwv/HDFS-SQOOP.png" alt="">

## Hive

Para poder trabajar con hive ingresamos al contenedor del hive-server.

Abrir un terminal y copiar el archivo hive.hql a hive-server

		docker cp datanode/scripts/hive/hive_importaciones_db.hql hive-server:/opt

Abrimos un terminal nuevo y ejecutamos lo siguiente

		docker exec -it hive-server bash

Para crear tablas externas en base a los datos importados con sqoop ejecutamos los siguientes pasos:

En el terminal de hive-server ejecutamos lo siguiente para crear las tablas

		hive -f /opt/hive.hql
  
<img src="https://i.postimg.cc/pXVPcxjh/tablas-hive.png" alt="">

En el terminal de hive-server ejecutamos lo siguiente para listar las base de datos de hive

		hive

En el terminal de hive-server  luego de haber ejecutado el comando hive ejecutamos lo siguiente

		show databases;
		use importaciones_db;
		show tables;

<img src="https://i.postimg.cc/W4pknxns/comandos-hive.png" alt="">

En la misma terminal de hive-server ejecutamos lo siguiente comando para verificar el contenido de las tablas en hive;

		select * from aduana_ingreso;
  
<img src="https://i.postimg.cc/Fzn4MktP/tablas-hive-select.png" alt="">

## Conexión a Hive en Hadoop utilizando Spark

Para trabajar con las tablas migradas de `importaciones_db` en Hadoop, utilizaremos Spark. A continuación, se detallan los pasos para conectarte a Hive y realizar análisis de datos utilizando PySpark desde Jupyter Notebook:

Una vez que los contenedores estén en funcionamiento, puedes utilizar Docker Explorer en Visual Studio Code para acceder a los puertos expuestos por los contenedores acá ubicaremos el puerto 8200 que es el puerto expuesto para nuestro Jupyter Notebook

<img src="https://i.postimg.cc/nzsmN5PB/puerto-expuestos.png" alt="">

Una vez que hayas abierto Jupyter Notebook utilizando Docker Explorer, encontrarás un archivo <b>SparkHive-checkpoint.ipynb.</b> Para acceder al archivo que contiene los códigos para conectarse a Hadoop y Hive, así como las queries utilizadas para el análisis de datos.

<img src="https://i.postimg.cc/zfMf9fWX/jupyter-query1.png" alt="">

<img src="https://i.postimg.cc/4NJX2cMQ/jupyter-query2.png" alt="">
