sqoop list-tables \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username root \
--password root

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table ADUANA_INGRESO \
--as-textfile \
--target-dir=/user/datapath/datasets/ADUANA_INGRESO \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table IMPORTACION \
--as-textfile \
--target-dir=/user/datapath/datasets/IMPORTACION \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table LINEA \
--as-textfile \
--target-dir=/user/datapath/datasets/LINEA \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table LINEA_MODELO \
--as-textfile \
--target-dir=/user/datapath/datasets/LINEA_MODELO \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table MARCA \
--as-textfile \
--target-dir=/user/datapath/datasets/MARCA \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table MODELO_LANZAMIENTO \
--as-textfile \
--target-dir=/user/datapath/datasets/MODELO_LANZAMIENTO \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table PAIS_ADUANA \
--as-textfile \
--target-dir=/user/datapath/datasets/PAIS_ADUANA \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table PAIS_ORIGEN \
--as-textfile \
--target-dir=/user/datapath/datasets/PAIS_ORIGEN \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table TIPO_COMBUSTIBLE \
--as-textfile \
--target-dir=/user/datapath/datasets/TIPO_COMBUSTIBLE \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table TIPO_IMPORTADOR \
--as-textfile \
--target-dir=/user/datapath/datasets/TIPO_IMPORTADOR \
--delete-target-dir

sqoop import \
--connect "jdbc:mysql://172.16.5.4:3310/importaciones_db" \
--username=root \
--password=root \
--table TIPO_VEHICULO \
--as-textfile \
--target-dir=/user/datapath/datasets/TIPO_VEHICULO \
--delete-target-dir
