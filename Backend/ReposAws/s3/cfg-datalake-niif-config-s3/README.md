 Despliegue Dinámico de Recursos Estáticos a S3
Este repositorio tiene como objetivo la gestión y despliegue automático de archivos de configuración (.json) y archivos estáticos (.html) hacia los buckets de Datalake Assets de forma dinámica.
Funcionamiento General
El despliegue se basa en el comando aws s3 sync. El pipeline de Jenkins toma todo el contenido dentro de la carpeta src/ y lo replica exactamente igual en el bucket S3 de destino.
•	Dinamismo total: No es necesario modificar el código del pipeline para agregar nuevas carpetas. Solo crea la carpeta en Git y el pipeline la creará en S3.
•	Espejo (Mirroring): Si un archivo se elimina de este repositorio, también se eliminará de S3 (Política de limpieza y ahorro de costos).
________________________________________
 Estructura del Repositorio (Git)
Para que el despliegue funcione, los archivos deben estar dentro de la carpeta src/. Cada subcarpeta dentro de src/ representará un folder en el bucket S3.
Plaintext
.
├── Jenkinsfile              # Configuración por ambientes (Dev, QA, Prd)
├── README.md                # Esta guía
└── src/                     # <--- CARPETA MÁGICA (Origen)
    ├── auxilios-funerarios/
    │   └── mapping.json
    ├── primas-arl/
    │   └── config.json
    └── reportes-niif/
        ├── index.html
        └── estilos.css
________________________________________
Resultado en AWS S3
Dependiendo de la configuración del target_bucket_path en el Jenkinsfile, la estructura se verá así:
Caso A: Despliegue en la Raíz (target_bucket_path: '')
Si se deja la ruta vacía, las carpetas de src/ quedan en el primer nivel del bucket.
•	s3://alfa-datalake-dev-assets/auxilios-funerarios/mapping.json
•	s3://alfa-datalake-dev-assets/primas-arl/config.json
Caso B: Despliegue con Prefijo (target_bucket_path: 'glue_resources')
Si se define una ruta base, todo se guarda adentro de ese folder.
•	s3://alfa-datalake-dev-assets/glue_resources/auxilios-funerarios/mapping.json
•	s3://alfa-datalake-dev-assets/glue_resources/primas-arl/config.json
________________________________________
Cómo agregar un nuevo recurso
1.	Crea una nueva carpeta dentro de src/ con el nombre que desees ver en S3 (ej. src/mis-nuevos-json/).
2.	Agrega tus archivos (.json, .html, etc.) dentro de esa carpeta.
3.	Realiza el Commit y Push a la rama correspondiente:
o	Rama develop -> Despliega en Bucket de DEV.
o	Rama quality -> Despliega en Bucket de QA.
o	Rama main -> Despliega en Bucket de PRD.
Bash
git add src/nueva-carpeta/
git commit -m "feat: agregando nuevos archivos de configuración"
git push origin develop
________________________________________
Configuración del Pipeline (Jenkinsfile)
El archivo Jenkinsfile controla a qué bucket se envía la información. Ejemplo de configuración:
Groovy
def envParams = [
    develop: [
        awsCredentialId: 'aws-datalake-dev',
        target_bucket_name: 'alfa-datalake-dev-assets',
        target_bucket_path: '' // Dejar vacío para desplegar en la raíz
    ],
    // ...
]
________________________________________
Notas Importantes (Best Practices)
•	FinOps & Limpieza: El pipeline usa la bandera --delete. Cuidado: Si borras un archivo o carpeta en este repositorio Git, desaparecerá de S3 en el próximo despliegue.
•	Formatos: Asegúrate de que los archivos .json tengan un formato válido antes de subir el cambio, ya que el pipeline no valida sintaxis, solo transfiere archivos.
•	Seguridad: Los permisos de acceso a estos archivos en S3 se gestionan mediante las políticas del Bucket o IAM Roles, no desde este repositorio.
