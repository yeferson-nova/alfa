# Código pendiente

El handler desplegado es `scheduler_configurators.lambda_handler`, pero su zip no se descomprimió (disco lleno en CloudShell). No hay copia local ni equivalente directo: el programador anterior es `niif-dev-programador-configuraciones` (en `Backend\Lamnbdas\pruebas-lambda\codigo\`), sin confirmar que haga lo mismo.

Recuperar con:
`aws lambda get-function --function-name niif-dev-manager-configuration --query Code.Location --output text | xargs curl -s -o manager.zip && unzip -j manager.zip 'scheduler_configurators.py'`
