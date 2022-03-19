package overlayfs

import (
	"os"

	"github.com/spf13/afero"
)

// Stat returns a FileInfo describing the named file, or an error, if any
// happens.
func (ofs *OverlayFs) Stat(name string) (os.FileInfo, error) {
	_, fi, _, err := ofs.stat(name, false)
	return fi, err
}

// LstatIfPossible will call Lstat if the filesystem iself is, or it delegates to, the os filesystem.
// Else it will call Stat.
func (ofs *OverlayFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	_, fi, ok, err := ofs.stat(name, false)
	return fi, ok, err
}

// Open opens a file, returning it or an error, if any happens.
// If name is a directory, a Dir is returned representing all directories matching name.
func (ofs *OverlayFs) Open(name string) (afero.File, error) {
	fs, fi, _, err := ofs.stat(name, false)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		var fss []afero.Fs
		if err := ofs.collectDirs(name, func(fs afero.Fs) {
			fss = append(fss, fs)
		}); err != nil {
			return nil, err
		}

		return &Dir{
			merge: ofs.mergeDirs,
			fss:   fss,
			name:  name,
		}, nil
	}

	return fs.Open(name)
}
