import textwrap

def lambda_handler(event, context):
    html = textwrap.dedent("""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <title>Post Login</title>
      <style>
        body { font-family: Arial, sans-serif; padding: 2rem; }
        #resultado { margin-top: 2rem; white-space: pre-wrap; }
      </style>
    </head>
    <body>
      <h2>Procesando solicitud...</h2>
      <div id="resultado"></div>
      <script>
        (async function () {
          try {
            const hash = window.location.hash.substring(1);
            const params = new URLSearchParams(hash);

            const id_token = params.get("id_token");
            const stateRaw = params.get("state");
            let state = {};
            try {
              state = JSON.parse(decodeURIComponent(stateRaw));
            } catch (e) {
              document.getElementById("resultado").innerText = "❌ Error al parsear state JSON: " + e;
              return;
            }
            const method = state.method;
            const resource = state.resource;
            const queryParams = state.queryParams || "";
            const body = state.body || {};

            const output = document.getElementById("resultado");

            if (!id_token || !state) {
              output.innerText = "❌ Faltan parámetros requeridos: id_token o state.";
              return;
            }

            if (!resource) {
              output.innerText = "❌ El parámetro 'resource' es obligatorio.";
              return;
            }

            let pathname = window.location.pathname;
            if (pathname.endsWith('/post_login')) {
              pathname = pathname.slice(0, -'/post_login'.length);
            }
            const baseUrl = window.location.origin + pathname;
            const fullUrl = queryParams ? `${baseUrl}${resource}?${queryParams}` : `${baseUrl}${resource}`;

            const headers = {
              "Authorization": `Bearer ${id_token}`,
              "Content-Type": "application/json"
            };

            let fetchOptions = { method, headers };

            if (["POST", "PUT", "PATCH", "DELETE"].includes(method)) {
              try {
                fetchOptions.body = JSON.stringify(body);
              } catch (e) {
                output.innerText = "❌ Error al parsear el cuerpo (body): " + e.message;
                return;
              }
            }

            const response = await fetch(fullUrl, fetchOptions);
            const contentType = response.headers.get("content-type") || "";

            let resultText;
            if (contentType.includes("application/json")) {
              const json = await response.json();
              resultText = JSON.stringify(json, null, 2);
            } else {
              resultText = await response.text();
            }

            output.innerText = `Llamada exitosa a ${resource}:\\n\\n` + resultText;

          } catch (err) {
            document.getElementById("resultado").innerText = "❌ Error inesperado:\\n" + err;
          }
        })();
      </script>
    </body>
    </html>
    """)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/html"
        },
        "body": html
    }
