Comando para migrar cada una de las migraciones.load:
docker run --rm --memory="8g" --memory-swap="8g" --name pgloader -v "ruta_a_este_directorio\migraciones:/mnt" -e SBCL_DYNAMIC_SPACE_SIZE=8589934592 ghcr.io/dimitri/pgloader:latest pgloader --dynamic-space-size 4096 /mnt/migracion_X.load

Los migraciones.load están configurados para migrar la BBDD desde un contenedor mysql a un contenedor postgresql

El dockerfile usado sirve para ejecutar la anonimización en el contenedor de POSTGRESQL una vez migrada la BBDD