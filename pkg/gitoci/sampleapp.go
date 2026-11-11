// Package gitoci provides the embedded fulfillment-api sample Go microservice
// and the in-cluster Bearer-authenticated Git Smart-HTTP + OCI Distribution v2
// server used by Tekton Actors pipelines.
package gitoci

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

//go:embed all:testdata/sample-app
var sampleAppFS embed.FS

//go:embed testdata/sample-app/pricing.go
var SampleAppCleanPricingGo string

//go:embed testdata/sample-app/pricing_buggy.go.tmpl
var SampleAppBuggyPricingGo string

const (
	// DefaultRepoName is the default Git and OCI repository name served by git-oci-server.
	DefaultRepoName = "fulfillment-api"

	// SampleAppWorkspaceRelPath is the path of pricing.go relative to /workspace
	// inside the release-pipeline Workspace Actor / Rescue Actor.
	SampleAppWorkspaceRelPath = "source/app/pricing.go"

	// SampleAppRepoFixDiff is the 1-line unified diff (relative to the repository root)
	// restoring exact discount subtraction in CalculateTotal.
	SampleAppRepoFixDiff = `--- a/pricing.go
+++ b/pricing.go
@@ -46,5 +46,5 @@
 	if discountCents >= subtotalCents {
 		return 0
 	}
-	return subtotalCents - discountCents - 1
+	return subtotalCents - discountCents
 }
`

	// SampleAppWorkspaceFixDiff is the 1-line unified diff relative to /workspace
	// accepted directly by Step Supervisor's POST /v1/rescue/patch endpoint on a Rescue Actor.
	SampleAppWorkspaceFixDiff = `--- a/source/app/pricing.go
+++ b/source/app/pricing.go
@@ -46,5 +46,5 @@
 	if discountCents >= subtotalCents {
 		return 0
 	}
-	return subtotalCents - discountCents - 1
+	return subtotalCents - discountCents
 }
`
)

// WriteSampleApp materializes the embedded fulfillment-api Go module into dstDir.
// When buggy is true, pricing.go contains the off-by-one cent discount regression
// caught by TestCalculateTotal in the unit-test task.
func WriteSampleApp(dstDir string, buggy bool) error {
	if strings.TrimSpace(dstDir) == "" {
		return fmt.Errorf("destination directory is required")
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dstDir, err)
	}
	root, err := os.OpenRoot(dstDir)
	if err != nil {
		return fmt.Errorf("open destination root %s: %w", dstDir, err)
	}
	defer root.Close()

	const basePrefix = "testdata/sample-app"
	entries, err := fs.ReadDir(sampleAppFS, basePrefix)
	if err != nil {
		return fmt.Errorf("read embedded sample-app: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "pricing_buggy.go.tmpl" {
			continue
		}
		targetName := name
		if name == "go.mod.tmpl" {
			targetName = "go.mod"
		}
		data, readErr := sampleAppFS.ReadFile(basePrefix + "/" + name)
		if readErr != nil {
			return fmt.Errorf("read embedded %s: %w", name, readErr)
		}
		if name == "pricing.go" && buggy {
			data = []byte(SampleAppBuggyPricingGo)
		}
		if writeErr := root.WriteFile(filepath.FromSlash(targetName), data, 0o644); writeErr != nil {
			return fmt.Errorf("write %s: %w", targetName, writeErr)
		}
	}
	return nil
}
