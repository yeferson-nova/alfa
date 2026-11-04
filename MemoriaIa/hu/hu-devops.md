---
tipo: hu
actualizado: 2026-11-03
---
# HUs Tema 04: DevOps y DataOps (HU-DO-01 a 20)

Fuente: `Docs\03-historias-usuario\04_HUs_Tecnicas_DevOps_DataOps.xlsx` (v1.0, oct-2026). Estándar obligatorio para todo repositorio, pipeline e IaC del frente de datos.

## Base normativa
Se basa en los lineamientos internos de Versionamiento V2 (entregado: V3, [[est-versionamiento-codigo]]), **CI/CD V1** e **IaC con Terraform GTC-GDD-004**. Estos dos últimos **no fueron entregados**, ver [[marco-normativo]].

## Reglas clave
- **Herramientas oficiales:** GitHub Enterprise, Jenkins con librerías compartidas, SonarQube, Nexus y Terraform/Terragrunt.
- **Ambientes:** DEV (`develop`, despliegue por pipeline), QA (`quality`, aprobación del gestor de ambientes) y PROD (`main`, lo ejecuta DevOps de Operación de Alfa).
- **"El proyecto declara, la plataforma ejecuta":**
  - `pipeline.yaml` declarativo y Jenkinsfile estándar;
  - **build once, deploy many**: el mismo artefacto se promueve DEV → QA → PROD;
  - tags inmutables: toda corrección genera un nuevo patch.
- **IaC:** módulos Terraform corporativos (HU-DO-09); Terragrunt con estado remoto (HU-DO-10); pipeline con plan en PR, apply desde rama protegida y **detección de deriva** (HU-DO-11).
- **Secretos (HU-DO-14):** ninguna credencial en texto plano en código, pipelines ni descriptores. Secrets Manager y KMS; Jenkins asume un rol por cuenta.
- **DevSecOps (HU-DO-15):** controles bloqueantes y homogéneos; políticas como código; excepciones formales.
- **Metadata, reglas de calidad y contratos como código** (HU-DO-12); pirámide de pruebas (HU-DO-13).
- **Rollback y hotfix** de código, infraestructura, metadata y datos (HU-DO-18).

## Convenciones (hoja "Convención de nombres")
- **Recurso AWS:** `<prefijo>-<dev|qa|stg|prd>-<proyecto>-<recurso>-<tipo>`, ≤ 64 caracteres. Ej.: `datalake-prd-niif-bronce-glue-job`. **Único ejemplo con "niif" en todos los documentos.**
- **Rol de despliegue:** `<prefijo>-<amb>-<proyecto>-despliegue-role`.
- **Estado de Terraform:** bucket `<prefijo>-<amb>-iac-tfstate-s3`.
- **Artefactos:** `<proyecto>/<componente>/vX.Y.Z/`.
- Repos, ramas y commits según [[est-versionamiento-codigo]], con expresiones regulares de validación.

## Decisiones
- **DO-10:** las notificaciones de pipelines van por correo corporativo y el canal de colaboración.
- **DO-12:** retención en Nexus: todo lo que se desplegó en PROD más las últimas 10 versiones.

## Relevancia ([[tareas-aws]])
- **Tarea 2:** el "modelo único de programación" de las lambdas debería alinearse con esta HU: plantilla de repositorio, pipeline, IaC y convenciones de nombres.
- Los cambios de SES, SNS, alarmas y API Gateway van como IaC por pipeline, **no por consola**.
- Brecha probable (propuesta): recursos actuales del paso 3 creados a mano o con nombres fuera del patrón. Por verificar en código.
