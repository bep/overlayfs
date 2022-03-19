package overlayfs

import (
	"os"

	"github.com/spf13/afero"
)

var (
	_ FilesystemIterator = (*OverlayFs)(nil)
	_ afero.Fs           = (*OverlayFs)(nil)
	_ afero.Lstater      = (*OverlayFs)(nil)
	_ afero.File         = (*Dir)(nil)
)

// FilesystemIterator is an interface for iterating over the wrapped filesystems in order.
type FilesystemIterator interface {
	Filesystem(i int) afero.Fs
	NumFilesystems() int
}

type Options struct {
	// The filesystems to overlay ordered in priority from left to right.
	Fss []afero.Fs

	// The OverlayFs is by default read-only, but you can nominate the first filesystem to be writable.
	FirstWritable bool

	// The DirsMerger is used to merge the contents of two directories.
	// If not provided, the defaultDirMerger is used.
	DirsMerger DirsMerger
}

// OverlayFs is a filesystem that overlays multiple filesystems.
// It's by default a read-only filesystem, but you can nominate the first filesystem to be writable.
// For all operations, the filesystems are checked in order until found.
// If a filesystem implementes FilesystemIterator, those filesystems will be checked before continuing.
type OverlayFs struct {
	fss []afero.Fs

	mergeDirs     DirsMerger
	firstWritable bool
}

// New creates a new OverlayFs with the given options.
func New(opts Options) *OverlayFs {
	if len(opts.Fss) == 0 {
		panic("no filesystems")
	}

	if opts.DirsMerger == nil {
		opts.DirsMerger = defaultDirMerger
	}

	return &OverlayFs{
		fss:           opts.Fss,
		mergeDirs:     opts.DirsMerger,
		firstWritable: opts.FirstWritable,
	}
}

// Append creates a shallow copy of the filesystem and appends the given filesystems to it.
func (ofs OverlayFs) Append(fss ...afero.Fs) *OverlayFs {
	ofs.fss = append(ofs.fss, fss...)
	return &ofs
}

// Filesystem returns filesystem with index i, nil if not found.
func (ofs *OverlayFs) Filesystem(i int) afero.Fs {
	if i >= len(ofs.fss) {
		return nil
	}
	return ofs.fss[i]
}

// NumFilesystems
func (ofs *OverlayFs) NumFilesystems() int {
	return len(ofs.fss)
}

// The name of this FileSystem.
func (ofs *OverlayFs) Name() string {
	return "overlayfs"
}

func (ofs *OverlayFs) collectDirs(name string, withFs func(fs afero.Fs)) error {
	for _, fs := range ofs.fss {
		if err := ofs.collectDirsRecursive(fs, name, withFs); err != nil {
			return err
		}
	}
	return nil
}

func (ofs *OverlayFs) collectDirsRecursive(fs afero.Fs, name string, withFs func(fs afero.Fs)) error {
	if fi, err := fs.Stat(name); err == nil && fi.IsDir() {
		withFs(fs)
	}
	if fsi, ok := fs.(FilesystemIterator); ok {
		for i := 0; i < fsi.NumFilesystems(); i++ {
			if err := ofs.collectDirsRecursive(fsi.Filesystem(i), name, withFs); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ofs *OverlayFs) stat(name string, lstatIfPossible bool) (afero.Fs, os.FileInfo, bool, error) {
	for _, fs := range ofs.fss {
		if fs2, fi, ok, err := ofs.statRecursive(fs, name, lstatIfPossible); err == nil {
			return fs2, fi, ok, nil
		}
	}
	return nil, nil, false, os.ErrNotExist
}

func (ofs *OverlayFs) statRecursive(fs afero.Fs, name string, lstatIfPossible bool) (afero.Fs, os.FileInfo, bool, error) {
	if lstatIfPossible {
		if lfs, ok := fs.(afero.Lstater); ok {
			fi, ok, err := lfs.LstatIfPossible(name)
			if err == nil {
				return fs, fi, ok, nil
			}
		} else if fi, err := fs.Stat(name); err == nil {
			return fs, fi, false, nil
		}
	} else if fi, err := fs.Stat(name); err == nil {
		return fs, fi, false, nil
	}
	if fsi, ok := fs.(FilesystemIterator); ok {
		for i := 0; i < fsi.NumFilesystems(); i++ {
			if fs2, fi, ok, err := ofs.statRecursive(fsi.Filesystem(i), name, lstatIfPossible); err == nil {
				return fs2, fi, ok, nil
			}
		}
	}
	return nil, nil, false, os.ErrNotExist
}

func (ofs *OverlayFs) writeFs() afero.Fs {
	return ofs.fss[0]
}

// DirsMerger is used to merge two directories.
type DirsMerger func(lofi, bofi []os.FileInfo) []os.FileInfo

var defaultDirMerger = func(lofi, bofi []os.FileInfo) []os.FileInfo {
	for _, bofi := range bofi {
		var found bool
		for _, lofi := range lofi {
			if bofi.Name() == lofi.Name() {
				found = true
				break
			}
		}
		if !found {
			lofi = append(lofi, bofi)
		}
	}
	return lofi

}

// Dir is an afero.File that represents list of directories that will be merged in Readdir and Readdirnames.
type Dir struct {
	name  string
	fss   []afero.Fs
	merge DirsMerger
}

func (d *Dir) Readdir(n int) ([]os.FileInfo, error) {
	if n > 0 {
		panic("Readdir with positive n not implemented")
	}
	fis := make([]os.FileInfo, 0, len(d.fss))
	readDir := func(fs afero.Fs) error {
		f, err := fs.Open(d.name)
		if err != nil {
			return err
		}
		defer f.Close()
		fi, err := f.Readdir(n)
		if err != nil {
			return err
		}
		fis = d.merge(fis, fi)
		return nil
	}

	for _, fs := range d.fss {
		if err := readDir(fs); err != nil {
			return nil, err
		}
	}

	return fis, nil

}

func (f *Dir) Readdirnames(n int) ([]string, error) {
	if n > 0 {
		panic("Readdirnames with positive n not implemented")
	}
	fis, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(fis))
	for i, fi := range fis {
		names[i] = fi.Name()
	}
	return names, nil
}

func (f *Dir) Stat() (os.FileInfo, error) {
	return f.fss[0].Stat(f.name)
}

func (f *Dir) Close() error {
	return nil
}

func (f *Dir) Name() string {
	return f.name
}

func (f *Dir) Read(p []byte) (n int, err error) {
	panic("not supported")
}

func (f *Dir) ReadAt(p []byte, off int64) (n int, err error) {
	panic("not supported")
}

func (f *Dir) Seek(offset int64, whence int) (int64, error) {
	panic("not supported")
}

func (f *Dir) Write(p []byte) (n int, err error) {
	panic("not supported")
}

func (f *Dir) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not supported")
}

func (f *Dir) Sync() error {
	panic("not supported")
}

func (f *Dir) Truncate(size int64) error {
	panic("not supported")
}

func (f *Dir) WriteString(s string) (ret int, err error) {
	panic("not supported")
}
