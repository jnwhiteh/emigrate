package emigrate

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)
import "os"

var pathNotFound = fmt.Errorf("Path not found")

// mocks an os.FileInfo
type mockFileInfo struct {
	name string
	size int64
}

func (m mockFileInfo) Name() string {
	return m.name
}

func (m mockFileInfo) Size() int64 {
	return m.size
}

func (m mockFileInfo) Mode() os.FileMode {
	return 0755
}

func (m mockFileInfo) ModTime() time.Time {
	return time.Now()
}

func (m mockFileInfo) IsDir() bool {
	return false
}

func (m mockFileInfo) Sys() interface{} {
	return nil
}

// mocks a filesystem, supporting ReadDir and ReadFile
type mockFilesystem struct {
	dirs map[string]map[string]string
}

func (m mockFilesystem) ReadDir(dir string) ([]os.FileInfo, error) {
	files, ok := m.dirs[dir]
	if !ok {
		return nil, pathNotFound
	}

	infos := make([]os.FileInfo, 0)
	for file, contents := range files {
		infos = append(infos, mockFileInfo{
			name: file,
			size: int64(len(contents)),
		})
	}

	return infos, nil
}

func (m mockFilesystem) ReadFile(file string) ([]byte, error) {
	dirname := filepath.Dir(file)
	filename := filepath.Base(file)

	dir, ok := m.dirs[dirname]
	if !ok {
		return nil, pathNotFound
	}
	contents, ok := dir[filename]
	if !ok {
		return nil, pathNotFound
	}
	return []byte(contents), nil
}

func TestPathNotFound(t *testing.T) {
	fs := mockFilesystem{}
	mf := migrationFinder{fs.ReadDir, fs.ReadFile}
	ms, err := mf.getMigrations("migrations")
	if ms != nil {
		t.Errorf("Expected no migrations")
	}
	if err != pathNotFound {
		t.Error("Expected %r got %r", pathNotFound, err)
	}
}

func TestDuplicateUpgrades(t *testing.T) {
	dirs := make(map[string]map[string]string)
	dirs["migrations"] = make(map[string]string)
	dirs["migrations"]["1_up.sql"] = ""
	dirs["migrations"]["01_up.sql"] = ""

	fs := mockFilesystem{dirs: dirs}
	mf := migrationFinder{fs.ReadDir, fs.ReadFile}
	ms, err := mf.getMigrations("migrations")

	_, ok := err.(DuplicateMigrationError)
	if err == nil || !ok {
		fmt.Printf("%r", err)
		t.Errorf("Expected duplicate migration error")
	}
	if ms != nil {
		t.Errorf("Expected no migrations, got %r", ms)
	}
}

func TestDuplicateDowngrades(t *testing.T) {
	dirs := make(map[string]map[string]string)
	dirs["migrations"] = make(map[string]string)
	dirs["migrations"]["1_down.sql"] = ""
	dirs["migrations"]["01_down.sql"] = ""

	fs := mockFilesystem{dirs: dirs}
	mf := migrationFinder{fs.ReadDir, fs.ReadFile}
	ms, err := mf.getMigrations("migrations")

	_, ok := err.(DuplicateMigrationError)
	if err == nil || !ok {
		t.Errorf("Expected duplicate migration error")
	}
	if ms != nil {
		t.Errorf("Expected no migrations, got %r", ms)
	}
}

func TestMissingUpgrade(t *testing.T) {
	dirs := make(map[string]map[string]string)
	dirs["migrations"] = make(map[string]string)
	dirs["migrations"]["001_down.sql"] = ""

	fs := mockFilesystem{dirs: dirs}
	mf := migrationFinder{fs.ReadDir, fs.ReadFile}
	ms, err := mf.getMigrations("migrations")

	_, ok := err.(MissingMigrationError)
	if err == nil || !ok {
		t.Errorf("Expected missing migration error")
	}
	if ms != nil {
		t.Errorf("Expected no migrations, got %r", ms)
	}
}

func TestMigrationsFromDir(t *testing.T) {
	dirs := make(map[string]map[string]string)
	dirs["migrations"] = make(map[string]string)
	dirs["migrations"]["001_up.sql"] = ""
	dirs["migrations"]["002_up.sql"] = ""
	dirs["migrations"]["003_up.sql"] = ""

	fs := mockFilesystem{dirs: dirs}
	mf := migrationFinder{fs.ReadDir, fs.ReadFile}
	_, err := mf.getMigrations("migrations")

	if err != nil {
		t.Errorf("Got unexpected error %#v", err)
	}
}
