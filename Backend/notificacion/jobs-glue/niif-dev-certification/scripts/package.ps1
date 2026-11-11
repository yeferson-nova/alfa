# Arma el paquete del job de Glue (igual que package.sh), para Windows PowerShell 5.1 o superior.
# Uso, desde la carpeta del job:  powershell -ExecutionPolicy Bypass -File scripts\package.ps1
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$raiz = Split-Path -Parent $PSScriptRoot
$nombre = 'niif-dev-certification'
$init = Join-Path $raiz 'src\certification_job\__init__.py'
$coincidencia = Select-String -Path $init -Pattern '^__version__ = "(.*)"' | Select-Object -First 1
if (-not $coincidencia) { throw "No se encontro __version__ en $init" }
$version = $coincidencia.Matches[0].Groups[1].Value

$destino = Join-Path $raiz 'dist'
$libreria = Join-Path $destino "$nombre-$version.zip"
$script = Join-Path $destino 'niif_certification.py'
New-Item -ItemType Directory -Force -Path $destino | Out-Null
foreach ($archivo in @($libreria, $script)) { if (Test-Path $archivo) { Remove-Item $archivo -Force } }

$origen = Join-Path $raiz 'src'
$paquetes = @('certification_job', 'niif_observability')
$zip = [System.IO.Compression.ZipFile]::Open($libreria, [System.IO.Compression.ZipArchiveMode]::Create)
try {
    foreach ($paquete in $paquetes) {
        $carpeta = Join-Path $origen $paquete
        if (-not (Test-Path $carpeta)) { throw "No existe el paquete $carpeta" }
        Get-ChildItem -Path $carpeta -Recurse -Filter '*.py' |
            Where-Object { $_.FullName -notmatch '\\__pycache__\\' } |
            ForEach-Object {
                # Rutas internas con "/" para que Python las importe en Glue (Linux)
                $relativa = $_.FullName.Substring($origen.Length + 1).Replace('\', '/')
                [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                    $zip, $_.FullName, $relativa, [System.IO.Compression.CompressionLevel]::Optimal) | Out-Null
            }
    }
}
finally {
    $zip.Dispose()
}
Copy-Item (Join-Path $raiz 'src\niif_certification.py') $script

Write-Output "Libreria: $libreria  (--extra-py-files)"
Write-Output "Script:   $script  (ScriptLocation del job)"
