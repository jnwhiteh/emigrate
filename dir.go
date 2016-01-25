package emigrate

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// DirMigrations returns a slice of migrations that can run against the files
// found in dir. An error is returned if the files cannot be read or if the
// files are erroneously named (such as no "up" migration existing or an
// unknown file extension).
func MigrationsFromDir(dir string) ([]Migration, error) {
	mf := migrationFinder{
		readDir:  ioutil.ReadDir,
		readFile: ioutil.ReadFile,
	}
	return mf.getMigrations(dir)
}

type migrationFinder struct {
	readDir  func(string) ([]os.FileInfo, error)
	readFile func(string) ([]byte, error)
}

// Used to enable testing, we can mock the ReadDir function and supply
func (mf migrationFinder) getMigrations(dir string) ([]Migration, error) {
	nameInfos, err := mf.groupByVersion(dir)
	if err != nil {
		return nil, err
	}

	// build a new Migration for each version
	ms := make([]Migration, 0, len(nameInfos))
	for _, names := range nameInfos {
		m, err := mf.getFileMigration(names)
		if err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}

	// sort the migrations
	sort.Sort(byVersion(ms))
	return ms, nil
}

// nameRegexp defines the file name pattern to recognize migration files
var nameRegexp = regexp.MustCompile(`^(\d+)[-_](up|down)\.([Ss][Qq][Ll])$`)

// nameInfo defines the information captured from parsing a file according to nameRegexp
type nameInfo struct {
	dir     string // file path
	name    string // file name
	version int64  // migration version
	way     string // "up" or "down"
	ext     string // file extension
}

// readDir collects and groups nameInfo by version, so that we can
// use this to detect inconsistencies in naming and having the same
// migration be used for both upgrading and downgrading.
func (mf migrationFinder) groupByVersion(dir string) (map[int64][]*nameInfo, error) {
	files, err := mf.readDir(dir)
	if err != nil {
		return nil, err
	}

	names := make(map[int64][]*nameInfo)
	for _, f := range files {
		// Skip if it's not a file
		if f.IsDir() {
			continue
		}

		name := f.Name()
		info, err := parseNameInfo(dir, name)
		if err != nil {
			return nil, err
		} else if info == nil {
			// File does not match nameRegexp
			continue
		}

		names[info.version] = append(names[info.version], info)
	}
	return names, nil
}

type MissingMigrationError struct {
	direction string
	version   int64
}

func (e MissingMigrationError) Error() string {
	return fmt.Sprintf("emigrate: Missing \"%s\" migration for version %d", e.direction, e.version)
}

type DuplicateMigrationError struct {
	direction string
	version   int64
}

func (e DuplicateMigrationError) Error() string {
	return fmt.Sprintf("emigrate: Duplicate \"%s\" migration for version %d", e.direction, e.version)
}

// getFileMigration returns a migration that upgrades or downgrades according
// to the files matching the given name infos.
func (mf migrationFinder) getFileMigration(names []*nameInfo) (Migration, error) {
	if len(names) == 0 || len(names) > 2 {
		// Logic error by caller
		log.Fatalf("getFileMigration called with invalid infos: %#v", names)
	}

	var m stringMigration
	m.version = names[0].version

	// Keep track of the directions we've seen for this version
	seen := make(map[string]bool)

	// Keep track of the extensions so they match
	ext := ""

	// For all files given, collect information about the migration and make sure
	// they are compatible with what we have already seen
	for _, info := range names {
		path := filepath.Join(info.dir, info.name)
		bytes, err := mf.readFile(path)
		if err != nil {
			return nil, err
		}
		contents := string(bytes)

		if ext != "" && ext != info.ext {
			return nil, fmt.Errorf("emigrate: Mixed extensions for migration version %d.", info.version)
		}
		ext = info.ext

		if info.way == "up" {
			if seen[info.way] {
				return nil, DuplicateMigrationError{"up", info.version}
			}
			m.up = contents
			seen[info.way] = true
		} else if info.way == "down" {
			if seen[info.way] {
				return nil, DuplicateMigrationError{"down", info.version}
			}
			m.down = contents
			seen[info.way] = true
		} else {
			// Logic error by caller
			log.Fatalf("getFileMigration called with unexpected way value: %#v", info)
		}
	}

	if !seen["up"] {
		return nil, MissingMigrationError{"up", m.version}
	}

	return m, nil
}

// parseNameInfo parses the name, returning a nameInfo.
// If the name is invalid an error is returned.
// If the name does not match the nameRegexp, nil is returned.
func parseNameInfo(dir, name string) (*nameInfo, error) {
	match := nameRegexp.FindStringSubmatch(name)
	if match == nil {
		return nil, nil
	}

	// Parse version number
	version, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || version < 1 {
		return nil, fmt.Errorf("emigrate: Version number of file %q is invalid.", name)
	}
	return &nameInfo{
		dir:     dir,
		name:    name,
		version: version,
		way:     match[2],
		ext:     match[3],
	}, nil
}
