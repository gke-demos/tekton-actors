// Package gitoci provides the embedded fulfillment-api sample Go microservice
// and the in-cluster Bearer/Basic-authenticated Git Smart-HTTP + OCI Distribution v2
// server shared across Tekton, Argo Workflows, and FluxCD on Agent Substrate.
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

	// DefaultTenantCount is the number of multi-tenant Kustomize overlays seeded in fulfillment-api.git.
	DefaultTenantCount = 24

	// SampleAppWorkspaceRelPath is the path of pricing.go relative to /workspace
	// inside the release-pipeline Workspace Actor / Rescue Actor.
	SampleAppWorkspaceRelPath = "source/app/pricing.go"

	// SampleAppRepoFixDiff is the 1-line unified diff (relative to the repository root)
	// restoring exact discount subtraction in CalculateTotal.
	SampleAppRepoFixDiff = `--- a/pricing.go
+++ b/pricing.go
@@ -53,5 +53,5 @@
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
@@ -53,5 +53,5 @@
 	if discountCents >= subtotalCents {
 		return 0
 	}
-	return subtotalCents - discountCents - 1
+	return subtotalCents - discountCents
 }
`
)

// WriteSampleApp materializes the embedded fulfillment-api Go module and Kustomize
// manifests (root + 24 tenant overlays under deploy/overlays/tenant-01..24) into dstDir.
// When buggy is true, pricing.go contains the off-by-one cent discount regression
// caught by TestCalculateTotal in the unit-test task.
func WriteSampleApp(dstDir string, buggy bool) error {
	return WriteSampleAppWithRelease(dstDir, buggy, "v1", 1)
}

// WriteSampleAppWithRelease materializes the embedded fulfillment-api Go module and
// Kustomize manifests with a specific release label and sequence counter.
func WriteSampleAppWithRelease(dstDir string, buggy bool, release string, sequence int) error {
	if strings.TrimSpace(dstDir) == "" {
		return fmt.Errorf("destination directory is required")
	}
	release = strings.TrimSpace(release)
	if release == "" {
		release = "v1"
	}
	if sequence <= 0 {
		sequence = 1
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

	// Write root Kustomize manifests (namespace omitted so Flux defaultUnsetNamespaces
	// targets each tenant's own namespace when reconciling root ./).
	rootFiles := buildKustomizeManifests("fulfillment-api", "", release, sequence)
	for name, content := range rootFiles {
		if err := root.WriteFile(filepath.FromSlash(name), []byte(content), 0o644); err != nil {
			return fmt.Errorf("write root %s: %w", name, err)
		}
	}

	// Write 24 multi-tenant Kustomize overlays under deploy/overlays/tenant-01..24.
	for i := 1; i <= DefaultTenantCount; i++ {
		tenantID := fmt.Sprintf("tenant-%02d", i)
		overlayRelDir := filepath.Join("deploy", "overlays", tenantID)
		if err := os.MkdirAll(filepath.Join(dstDir, overlayRelDir), 0o755); err != nil {
			return fmt.Errorf("mkdir overlay %s: %w", overlayRelDir, err)
		}
		tenantFiles := buildKustomizeManifests(tenantID, tenantID, release, sequence)
		for name, content := range tenantFiles {
			relPath := filepath.Join(overlayRelDir, name)
			if err := root.WriteFile(relPath, []byte(content), 0o644); err != nil {
				return fmt.Errorf("write overlay %s: %w", relPath, err)
			}
		}
	}

	return nil
}

// WriteOOMOverlay writes a dense Kustomize configMapGenerator amplification overlay
// (~280 KiB payload repeated across 16 generated ConfigMaps -> ~4.5 MiB rendered YAML)
// into root and the target tenant overlay directory to test gVisor sandbox memory isolation.
func WriteOOMOverlay(dstDir, tenantID string) error {
	if strings.TrimSpace(dstDir) == "" {
		return fmt.Errorf("destination directory is required")
	}
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		tenantID = "tenant-01"
	}

	line := fmt.Sprintf("tenant=%s-runaway-kustomize-amplification-block-0123456789abcdef\n", tenantID)
	repeats := (280 << 10) / len(line)
	if repeats < 1 {
		repeats = 4096
	}
	payload := strings.Repeat(line, repeats)

	buildBombKustomization := func(ns string) string {
		var kb strings.Builder
		kb.WriteString("apiVersion: kustomize.config.k8s.io/v1beta1\n")
		kb.WriteString("kind: Kustomization\n")
		if ns != "" {
			kb.WriteString("namespace: " + ns + "\n")
		}
		kb.WriteString("configMapGenerator:\n")
		for i := range 16 {
			fmt.Fprintf(&kb, "  - name: runaway-shard-%03d\n    files:\n      - payload.txt\n", i)
		}
		return kb.String()
	}

	if err := os.WriteFile(filepath.Join(dstDir, "kustomization.yaml"), []byte(buildBombKustomization("")), 0o644); err != nil {
		return fmt.Errorf("write root oom kustomization.yaml: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "payload.txt"), []byte(payload), 0o644); err != nil {
		return fmt.Errorf("write root oom payload.txt: %w", err)
	}

	overlayDir := filepath.Join(dstDir, "deploy", "overlays", tenantID)
	if err := os.MkdirAll(overlayDir, 0o755); err != nil {
		return fmt.Errorf("mkdir oom overlay %s: %w", overlayDir, err)
	}
	if err := os.WriteFile(filepath.Join(overlayDir, "kustomization.yaml"), []byte(buildBombKustomization(tenantID)), 0o644); err != nil {
		return fmt.Errorf("write tenant oom kustomization.yaml: %w", err)
	}
	if err := os.WriteFile(filepath.Join(overlayDir, "payload.txt"), []byte(payload), 0o644); err != nil {
		return fmt.Errorf("write tenant oom payload.txt: %w", err)
	}
	return nil
}

func buildKustomizeManifests(tenantLabel, namespace, release string, sequence int) map[string]string {
	nsLine := ""
	if namespace != "" {
		nsLine = fmt.Sprintf("  namespace: %s\n", namespace)
	}
	return map[string]string{
		"kustomization.yaml": fmt.Sprintf(`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
labels:
  - pairs:
      app.kubernetes.io/managed-by: flux-substrate
      reconciler.ate.dev/tenant: %s
resources:
  - configmap.yaml
  - service.yaml
  - deployment.yaml
`, tenantLabel),
		"configmap.yaml": fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
%sdata:
  app: "fulfillment-api"
  tenant: %q
  release: %q
  channel: "stable"
  sequence: "%d"
`, nsLine, tenantLabel, release, sequence),
		"service.yaml": fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: tenant-workload
%sspec:
  selector:
    app.kubernetes.io/name: tenant-workload
    reconciler.ate.dev/tenant: %s
  ports:
    - name: http
      port: 80
      targetPort: 8080
`, nsLine, tenantLabel),
		"deployment.yaml": fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: tenant-workload
%sspec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: tenant-workload
      reconciler.ate.dev/tenant: %s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tenant-workload
        reconciler.ate.dev/tenant: %s
    spec:
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.10
          ports:
            - containerPort: 8080
`, nsLine, tenantLabel, tenantLabel),
	}
}
