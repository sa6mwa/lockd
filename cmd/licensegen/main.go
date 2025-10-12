package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type module struct {
	Path     string  `json:"Path"`
	Version  string  `json:"Version"`
	Dir      string  `json:"Dir"`
	Main     bool    `json:"Main"`
	Indirect bool    `json:"Indirect"`
	Replace  *module `json:"Replace"`
}

type licenseBlob struct {
	ModulePath string
	Version    string
	Files      []string
	Content    map[string][]byte
	Indirect   bool
}

func main() {
	output := flag.String("o", "THIRD_PARTY_LICENSES.md", "output file path")
	includeIndirect := flag.Bool("indirect", true, "include indirect modules")
	flag.Parse()

	modules, err := listModules()
	if err != nil {
		fmt.Fprintf(os.Stderr, "licensegen: %v\n", err)
		os.Exit(1)
	}

	var blobs []licenseBlob
	for _, m := range modules {
		if m.Main {
			continue
		}
		if m.Indirect && !*includeIndirect {
			continue
		}
		dir := effectiveDir(m)
		if dir == "" {
			continue
		}
		files := findLicenseFiles(dir)
		if len(files) == 0 {
			continue
		}
		content := map[string][]byte{}
		for _, name := range files {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				fmt.Fprintf(os.Stderr, "licensegen: read %s: %v\n", filepath.Join(dir, name), err)
				os.Exit(1)
			}
			content[name] = normalizeNewlines(data)
		}
		blobs = append(blobs, licenseBlob{
			ModulePath: m.Path,
			Version:    m.Version,
			Files:      files,
			Content:    content,
			Indirect:   m.Indirect,
		})
	}

	sort.Slice(blobs, func(i, j int) bool {
		if blobs[i].ModulePath == blobs[j].ModulePath {
			return blobs[i].Version < blobs[j].Version
		}
		return blobs[i].ModulePath < blobs[j].ModulePath
	})

	if err := writeReport(*output, blobs); err != nil {
		fmt.Fprintf(os.Stderr, "licensegen: %v\n", err)
		os.Exit(1)
	}
}

func listModules() ([]module, error) {
	cmd := exec.Command("go", "list", "-m", "-json", "all")
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	var modules []module
	dec := json.NewDecoder(out)
	for {
		var m module
		if err := dec.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		modules = append(modules, m)
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}
	return modules, nil
}

func effectiveDir(m module) string {
	if m.Replace != nil {
		if dir := effectiveDir(*m.Replace); dir != "" {
			return dir
		}
	}
	return m.Dir
}

func findLicenseFiles(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if isLicenseFile(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func isLicenseFile(name string) bool {
	n := strings.ToLower(name)
	switch n {
	case "license", "license.txt", "license.md", "licence", "licence.txt", "licence.md",
		"copying", "copying.txt", "copying.md", "unlicense", "unlicense.txt":
		return true
	}
	if strings.HasPrefix(n, "license-") || strings.HasPrefix(n, "licence-") || strings.HasPrefix(n, "notice") {
		return true
	}
	return false
}

func normalizeNewlines(b []byte) []byte {
	b = bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n"))
	return b
}

func writeReport(path string, blobs []licenseBlob) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	defer writer.Flush()

	fmt.Fprintf(writer, "# Third-Party Licenses\n\n")
	fmt.Fprintf(writer, "_Generated on %s using `go list -m all`._\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(writer, "Lockd is distributed under the MIT License. The following third-party modules are bundled at build or runtime and their original license terms are reproduced below.\n\n")

	if len(blobs) > 0 {
		fmt.Fprintf(writer, "## Summary\n\n")
		fmt.Fprintf(writer, "| Module | Version | License Files | Indirect |\n")
		fmt.Fprintf(writer, "| --- | --- | --- | --- |\n")
		for _, blob := range blobs {
			fmt.Fprintf(writer, "| %s | %s | %s | %t |\n",
				blob.ModulePath, blob.Version, strings.Join(blob.Files, ", "), blob.Indirect)
		}
		fmt.Fprintln(writer)
	}

	for _, blob := range blobs {
		fmt.Fprintf(writer, "## %s %s\n\n", blob.ModulePath, blob.Version)
		if blob.Indirect {
			fmt.Fprintf(writer, "_This module is an indirect dependency._\n\n")
		}
		for _, name := range blob.Files {
			fmt.Fprintf(writer, "### %s\n\n", name)
			fmt.Fprintf(writer, "```text\n%s```\n\n", strings.TrimRight(string(blob.Content[name]), "\n"))
		}
	}
	return nil
}
