param(
    [string]$Origen = (Join-Path $PSScriptRoot '_clones')
)
$ErrorActionPreference = 'Stop'

$destino = $PSScriptRoot
$alfa = (Resolve-Path (Join-Path $destino '..\..')).Path
$origenFull = (Resolve-Path $Origen).Path
$tipos = @{ 'sc-' = 'lambdas'; 'data-' = 'glue'; 'cfg-' = 's3'; 'tpl-' = 's3' }

Push-Location $alfa
try {
    git check-ignore -q $origenFull
    if ($LASTEXITCODE -ne 0 -and $origenFull.StartsWith($alfa, [StringComparison]::OrdinalIgnoreCase)) {
        throw "El origen $origenFull esta dentro de alfa y Git no lo ignora. Agrega la carpeta a .gitignore."
    }

    $anidados = Get-ChildItem $destino -Directory |
        Where-Object { $_.FullName -ne $origenFull -and (Test-Path (Join-Path $_.FullName '.git')) }
    if ($anidados) {
        $nombres = ($anidados | ForEach-Object Name) -join ', '
        throw "Hay clones con .git fuera de ${origenFull}: $nombres. Muevelos a esa carpeta y vuelve a correr."
    }

    $enlaces = git ls-files -s Backend/ReposAws | Where-Object { $_ -match '^160000' }
    foreach ($linea in $enlaces) {
        $ruta = ($linea -split "`t", 2)[1]
        git rm --cached --quiet $ruta
        "Enlace quitado: $ruta"
    }

    $resumen = @()
    foreach ($repo in Get-ChildItem $origenFull -Directory | Where-Object { Test-Path (Join-Path $_.FullName '.git') }) {
        $prefijo = $tipos.Keys | Where-Object { $repo.Name.StartsWith($_) } | Select-Object -First 1
        $tipo = if ($prefijo) { $tipos[$prefijo] } else { 'otros' }
        $salida = Join-Path $destino (Join-Path $tipo $repo.Name)
        robocopy $repo.FullName $salida /MIR /XD .git /NFL /NDL /NJH /NJS /NP | Out-Null
        if ($LASTEXITCODE -ge 8) { throw "robocopy fallo en $($repo.Name) (codigo $LASTEXITCODE)" }
        $rama = git -C $repo.FullName rev-parse --abbrev-ref HEAD
        $commit = git -C $repo.FullName rev-parse HEAD
        $fecha = git -C $repo.FullName log -1 --format=%cI
        $resumen += "| $tipo | $($repo.Name) | $rama | $commit | $fecha |"
    }

    $tabla = @('# Version copiada de cada repo', '', "Copiado: $(Get-Date -Format s)", '',
        '| Tipo | Repo | Rama | Commit | Fecha del commit |', '|---|---|---|---|---|') + $resumen
    $tabla | Set-Content -Encoding utf8 (Join-Path $destino 'VERSIONES.md')

    git add Backend/ReposAws
    git status --short Backend/ReposAws | Select-Object -First 40
    "Listo. Revisa el estado y haz commit y push."
}
finally {
    Pop-Location
}
