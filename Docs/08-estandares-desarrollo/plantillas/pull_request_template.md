<!--
Antes de enviar: revisa que la BASE del PR sea la correcta (GitHub propone main por defecto)
y que el título siga el formato <tipo>-<TICKET>-<asunto>.
Lo que no aplique se deja con "no aplica" y una frase; no se borra la sección.
-->

## Parámetros
| Campo | Valor |
|---|---|
| Tipo | feat / fix / hotfix / refactor / perf / test / docs / ci / chore |
| Alcance (repo o módulo) | |
| Ticket o ID de brecha | |
| Base | develop / quality / main |
| Ambiente donde se probó | DEV / QA / local |
| Riesgo | bajo / medio / alto |
| Requiere cambio de configuración o IaC | no / sí: |
| Se puede revertir | sí: revert del PR / no: |

## Qué cambia y por qué
<!-- Una a tres frases: qué problema resuelve y qué comportamiento cambia. -->

## Cómo se probó
<!-- Comandos o pruebas y su resultado. Evidencia: salida, captura o enlace. -->

## Impacto
- Datos o cifras afectadas:
- Configuración nueva o modificada (claves del JSON, argumentos):
- Costo o rendimiento (tiempo, DPU-horas, bytes escaneados de Athena):
- Otros repos o equipos que deben enterarse:

## Cómo revertirlo
<!-- Qué se hace si algo sale mal después de fusionar. -->

## Lista de verificación
**Todos**
- [ ] Rama, commits y título siguen el formato del estándar (V3)
- [ ] Sin secretos, datos personales ni valores quemados
- [ ] Pruebas nuevas o ajustadas; pipeline en verde; cobertura del código nuevo > 80 %
- [ ] README o documentación actualizados
- [ ] El PR cuenta un solo tema y tiene menos de 400 líneas (o se justifica)

**Jobs de Glue** (borrar el bloque si no aplica)
- [ ] Columnas explícitas, filtro por periodo y particiones
- [ ] Compuerta de calidad antes de escribir; cuadre entra = sale + cuarentena
- [ ] Importes en DECIMAL con precisión y redondeo declarados
- [ ] Impacto en tiempo y costo declarado

**Lambdas** (borrar el bloque si no aplica)
- [ ] Estructura hexagonal y prueba de arquitectura en verde; handler sin lógica
- [ ] Errores con taxonomía, ninguno silenciado; idempotente
- [ ] Permisos IAM mínimos

**Código nuevo en general**
- [ ] Parámetros que pueden cambiar están en configuración y se actualizan sin desplegar
- [ ] Llamados externos con timeout y reintentos acotados
- [ ] Un evento de log por paso con `ejecucion_id`

**IaC** (borrar el bloque si no aplica)
- [ ] Plan adjunto; nada creado a mano en consola; etiquetas de costos y gobierno

## Para el revisor
- [ ] Leí el código y la descripción
- [ ] La base y el alcance son correctos
- [ ] Los comentarios quedaron respondidos
