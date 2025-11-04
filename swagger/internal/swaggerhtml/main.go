package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	specPath := flag.String("spec", "", "path to swagger.json")
	outPath := flag.String("out", "", "path to generated swagger HTML")
	flag.Parse()

	if *specPath == "" {
		fmt.Fprintln(os.Stderr, "missing -spec path")
		os.Exit(2)
	}
	if *outPath == "" {
		fmt.Fprintln(os.Stderr, "missing -out path")
		os.Exit(2)
	}

	specBytes, err := os.ReadFile(*specPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read spec: %v\n", err)
		os.Exit(1)
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, specBytes); err != nil {
		fmt.Fprintf(os.Stderr, "compact spec: %v\n", err)
		os.Exit(1)
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>lockd API reference</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  <style>
    body { margin: 0; background: #f7f7f7; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
  <script>
    window.onload = function() {
      const spec = %s;
      SwaggerUIBundle({
        spec: spec,
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
        layout: 'BaseLayout'
      });
    };
  </script>
</body>
</html>
`, compact.String())

	if err := os.WriteFile(*outPath, []byte(html), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write html: %v\n", err)
		os.Exit(1)
	}
}
