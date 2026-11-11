# Arma el paquete de la lambda (igual que package.sh), para Windows PowerShell 5.1 o superior.
# Uso, desde la carpeta de la lambda:  powershell -ExecutionPolicy Bypass -File scripts\package.ps1
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$raiz = Split-Path -Parent $PSScriptRoot
$nombre = 'niif-dev-notification-certification-orchestrator'
$init = Join-Path $raiz 'src\certification_orchestrator\__init__.py'
$coincidencia = Select-String -Path $init -Pattern '^__version__ = "(.*)"' | Select-Object -First 1
if (-not $coincidencia) { throw "No se encontro __version__ en $init" }
$version = $coincidencia.Matches[0].Groups[1].Value

$destino = Join-Path $raiz 'dist'
$paquete = Join-Path $destino "$nombre-$version.zip"
New-Item -ItemType Directory -Force -Path $destino | Out-Null
if (Test-Path $paquete) { Remove-Item $paquete -Force }

$origen = Join-Path $raiz 'src'
$zip = [System.IO.Compression.ZipFile]::Open($paquete, [System.IO.Compression.ZipArchiveMode]::Create)
try {
    [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
        $zip, (Join-Path $origen 'lambda_function.py'), 'lambda_function.py',
        [System.IO.Compression.CompressionLevel]::Optimal) | Out-Null
    foreach ($carpetaPaquete in @('certification_orchestrator', 'niif_observability')) {
        $carpeta = Join-Path $origen $carpetaPaquete
        if (-not (Test-Path $carpeta)) { throw "No existe el paquete $carpeta" }
        Get-ChildItem -Path $carpeta -Recurse -Filter '*.py' |
            Where-Object { $_.FullName -notmatch '\\__pycache__\\' } |
            ForEach-Object {
                # Rutas internas con "/" para que Python las importe en Lambda (Linux)
                $relativa = $_.FullName.Substring($origen.Length + 1).Replace('\', '/')
                [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                    $zip, $_.FullName, $relativa, [System.IO.Compression.CompressionLevel]::Optimal) | Out-Null
            }
    }
}
finally {
    $zip.Dispose()
}
Write-Output "Paquete de la lambda: $paquete  (handler: lambda_function.lambda_handler)"
