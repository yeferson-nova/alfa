# Comandos de alfa-repos

Ciclo para pasar cambios entre el PC personal y el PC corporativo con el repo `alfa-repos`. El detalle y las reglas están en el `README.md` de `alfa-repos`.

## 1. Corporativo: exportar `quality` a alfa-repos

```powershell
cd C:\Users\ynova\Git\alfa-repos
git pull

# todos los repos
powershell -ExecutionPolicy Bypass -File .\scripts\exportar.ps1 -Origen C:\Users\ynova\Git\ReposAws

# o solo uno o varios
powershell -ExecutionPolicy Bypass -File .\scripts\exportar.ps1 -Origen C:\Users\ynova\Git\ReposAws -Repos "sc-datalake-niif-certification,sc-datalake-niif-primas-arl"

git push
```

## 2. Personal (aquí): trabajar y generar los parches

```powershell
cd C:\Users\rdpuser\Git\alfa-repos
git switch main; git pull
git switch -c feature/T1-ses
# ... cambios y commits ...
powershell -ExecutionPolicy Bypass -File .\scripts\generar-parches.ps1 -Rama feature/T1-ses
git push origin main feature/T1-ses --follow-tags --force-with-lease
```

## 3. Corporativo: aplicar los parches en los repos de Alfa

```powershell
cd C:\Users\ynova\Git\alfa-repos
git pull
powershell -ExecutionPolicy Bypass -File .\scripts\aplicar-parches.ps1 -Rama feature/T1-ses -Origen C:\Users\ynova\Git\ReposAws
# en cada repo tocado: git push -u origin feature/T1-ses  -> PR a quality
```

## Opcional: no escribir `-Origen` cada vez

Se define una sola vez en el PC corporativo. Después de abrir una terminal nueva se puede omitir `-Origen` en `exportar` y en `aplicar`:

```powershell
setx ALFA_REPOS_ORIGEN C:\Users\ynova\Git\ReposAws
```
