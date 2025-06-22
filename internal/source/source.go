// internal/source/source.go
package source

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// InputSource defines an abstract source for data, providing a way to get
// a streaming reader for the content, its path, and its size.
type InputSource interface {
	Path() string
	Open(ctx context.Context) (io.ReadCloser, error)
	Dir() string
	Size() int64
}

// DiscoverAll iterates through a list of path strings, calls Discover for each,
// and aggregates the results, ensuring no source is included more than once.
// It returns an error if any path is invalid.
func DiscoverAll(ctx context.Context, paths []string) ([]InputSource, error) {
	var uniqueSources []InputSource
	discoveredPaths := make(map[string]bool)

	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		sources, err := Discover(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("error in path '%s': %w", p, err)
		}

		for _, s := range sources {
			canonicalPath := s.Path()
			if !discoveredPaths[canonicalPath] {
				discoveredPaths[canonicalPath] = true
				uniqueSources = append(uniqueSources, s)
			}
		}
	}

	if len(uniqueSources) == 0 {
		return nil, fmt.Errorf("no processable files found in any of the provided paths")
	}
	return uniqueSources, nil
}

// Discover finds all relevant sources at a given path, dispatching to the correct
// implementation based on the path prefix (e.g., "gs://").
func Discover(ctx context.Context, path string) ([]InputSource, error) {
	if strings.HasPrefix(path, "gs://") {
		return discoverGCSObjects(ctx, path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("local path is not a directory: %s", path)
	}
	return discoverLocalFiles(ctx, path)
}

// LocalFileSource implements InputSource for the local filesystem.
type LocalFileSource struct {
	filePath string
	size     int64
}

// Path returns the full file path.
func (lfs LocalFileSource) Path() string { return lfs.filePath }

// Open returns an os.File reader.
func (lfs LocalFileSource) Open(_ context.Context) (io.ReadCloser, error) { return os.Open(lfs.filePath) }

// Dir returns the containing directory of the file.
func (lfs LocalFileSource) Dir() string { return filepath.Dir(lfs.filePath) }

// Size returns the size of the file in bytes.
func (lfs LocalFileSource) Size() int64 { return lfs.size }

// GCSObjectSource implements InputSource for Google Cloud Storage objects.
type GCSObjectSource struct {
	bucket *storage.BucketHandle
	object *storage.ObjectAttrs
}

// Path returns the full gs:// URI for the object.
func (gcs GCSObjectSource) Path() string {
	return fmt.Sprintf("gs://%s/%s", gcs.object.Bucket, gcs.object.Name)
}

// Open returns a new streaming reader for the GCS object.
func (gcs GCSObjectSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return gcs.bucket.Object(gcs.object.Name).NewReader(ctx)
}

// Dir returns the containing "directory" (prefix) of the object within its bucket.
func (gcs GCSObjectSource) Dir() string { return filepath.Dir(gcs.Path()) }

// Size returns the size of the GCS object in bytes.
func (gcs GCSObjectSource) Size() int64 {
	return gcs.object.Size
}

func discoverGCSObjects(ctx context.Context, path string) ([]InputSource, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w. Ensure you are authenticated", err)
	}
	defer client.Close()

	trimmedPath := strings.TrimPrefix(path, "gs://")
	parts := strings.SplitN(trimmedPath, "/", 2)
	bucketName := parts[0]
	var prefix string
	if len(parts) > 1 {
		prefix = parts[1]
	}
	if bucketName == "" {
		return nil, fmt.Errorf("invalid GCS path: bucket name cannot be empty in '%s'", path)
	}

	bucket := client.Bucket(bucketName)

	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, fmt.Errorf("GCS bucket '%s' not found or access denied: %w", bucketName, err)
	}

	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)
	var sources []InputSource

	allowedMimeTypes := map[string]bool{
		"application/json":           true,
		"application/x-ndjson":       true,
		"application/json-seq":       true,
		"application/jsonlines":      true,
		"application/jsonlines+json": true,
		"application/x-jsonlines":    true,
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate GCS objects in bucket '%s': %w", bucketName, err)
		}
		if ctx.Err() != nil {
			return nil, context.Canceled
		}
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		if allowedMimeTypes[attrs.ContentType] {
			sources = append(sources, GCSObjectSource{bucket: bucket, object: attrs})
		}
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no processable JSON files found in 'gs://%s' with prefix '%s'", bucketName, prefix)
	}
	return sources, nil
}

func discoverLocalFiles(ctx context.Context, dirPath string) ([]InputSource, error) {
	var sources []InputSource
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if ctx.Err() != nil {
			return context.Canceled
		}
		if err != nil {
			return err
		}
		if !info.IsDir() && (strings.HasSuffix(strings.ToLower(path), ".json") || strings.HasSuffix(strings.ToLower(path), ".ndjson") || strings.HasSuffix(strings.ToLower(path), ".jsonl")) {
			absPath, err := filepath.Abs(path)
			if err != nil {
				return fmt.Errorf("could not get absolute path for %s: %w", path, err)
			}
			sources = append(sources, LocalFileSource{filePath: absPath, size: info.Size()})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk local directory %q: %w", dirPath, err)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no .json, .ndjson, or .jsonl files found in %s", dirPath)
	}
	return sources, nil
}
