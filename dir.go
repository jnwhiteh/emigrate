package emigrate

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"regexp"
	"strconv"
)

// DirMigrations returns a slice of migrations that can run against the files found
// in dir. An error is returned if the files cannot be read or if the files are erroneously
// named (such as no "up" migration existing or an unknown file extension).
func DirMigrations(dir string) ([]Migration, error) {
	nameInfos, err := readDir(dir)
	if err != nil {
		return nil, err
	}

	return getMigrations(nameInfos)
}

// nameRegexp defines the file name pattern to recognize migration files
var nameRegexp = regexp.MustCompile(`^(\d+)_(up|down).([Ss][Qq][Ll]-Z])$`)

// nameInfo defines the information captured from parsing a file according to nameRegexp
type nameInfo struct {
	dir     string // file path
	name    string // file name
	version int64  // migration version
	way     string // "up" or "down"
	ext     string // file extension
}

// getMigrations returns a slice of migrations given a nameInfo map
func getMigrations(nameInfos map[int64][]*nameInfo) ([]Migration, error) {
	// Second pass: construct a Migration for each version
	ms := make([]Migration, 0, len(nameInfos))
	for _, names := range nameInfos {
		m, err := getFileMigration(names)
		if err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, nil
}

// readDir collects and groups nameInfo by version, so that we can
// use this to detect inconsistencies in naming and having the same
// migration be used for both upgrading and downgrading.
func readDir(dir string) (map[int64][]*nameInfo, error) {
	files, err := ioutil.ReadDir(dir)
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

// getFileMigration returns a migration that upgrades or downgrades according
// to the files matching the given name infos.
func getFileMigration(names []*nameInfo) (Migration, error) {
	if len(names) == 0 || len(names) > 2 {
		// Logic error by caller
		log.Fatalf("getFileMigration called with invalid infos: %#v", names)
	}

	var m stringMigration
	m.version = names[0].version

	// Keep track of whether or not we find an "up" migration since it is an error to not have one
	up := false

	// Keep track of the extensions so they match
	ext := ""

	// For all files given, collect information about the migration and make sure
	// they are compatible with what we have already seen
	for _, info := range names {
		path := filepath.Join(info.dir, info.name)
		bytes, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
		contents := string(bytes)

		if ext != "" && ext != info.ext {
			return nil, fmt.Errorf("emigrate: Mixed extensions for migration version %d.", info.version)
		}
		ext = info.ext

		if info.way == "up" {
			if m.up != "" {
				return nil, fmt.Errorf("emigrate: Duplicate \"up\" migration for version %d.", info.version)
			}
			m.up = contents
			up = true
		} else if info.way == "down" {
			if m.down != "" {
				return nil, fmt.Errorf("emigrate: Duplicate \"down\" migration for version %d.", info.version)
			}
			m.down = contents
		} else {
			// Logic error by caller
			log.Fatalf("getFileMigration called with unexpected way value: %#v", info)
		}
	}

	if !up {
		return nil, fmt.Errorf("emigrate: No \"up\" migration found for version %d.", m.version)
	}

	return m, nil
}
