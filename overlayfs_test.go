// Copyright 2025 Bjørn Erik Pedersen
// SPDX-License-Identifier: MIT

package overlayfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/afero"
	"golang.org/x/tools/txtar"
)

func TestAppend(t *testing.T) {
	c := qt.New(t)
	ofs1 := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "1")}})
	ofs2 := ofs1.Append(basicFs("3", "1"))
	c.Assert(ofs1.NumFilesystems(), qt.Equals, 2)
	c.Assert(ofs2.NumFilesystems(), qt.Equals, 3)
	c.Assert(readDirnames(c, ofs1, "mydir"), qt.DeepEquals, []string{"f1-1.txt", "f2-1.txt", "f1-2.txt", "f2-2.txt"})
	c.Assert(readDirnames(c, ofs2, "mydir"), qt.DeepEquals, []string{"f1-1.txt", "f2-1.txt", "f1-2.txt", "f2-2.txt", "f1-3.txt", "f2-3.txt"})
}

func TestWithDirsMerger(t *testing.T) {
	c := qt.New(t)

	ofs1 := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "1")}})
	ofs2 := ofs1.WithDirsMerger(func(lofi, bofi []fs.DirEntry) []fs.DirEntry {
		if len(lofi) == 0 {
			return bofi
		}
		return lofi[:2]
	})
	ofs1 = ofs1.Append(basicFs("3", "1"))
	c.Assert(ofs1.NumFilesystems(), qt.Equals, 3)
	c.Assert(ofs2.NumFilesystems(), qt.Equals, 2)
	c.Assert(readDirnames(c, ofs1, "mydir"), qt.DeepEquals, []string{"f1-1.txt", "f2-1.txt", "f1-2.txt", "f2-2.txt", "f1-3.txt", "f2-3.txt"})
	c.Assert(readDirnames(c, ofs2, "mydir"), qt.DeepEquals, []string{"f1-1.txt", "f2-1.txt"})
}

func TestEmpty(t *testing.T) {
	c := qt.New(t)
	ofs := New(Options{FirstWritable: true})
	c.Assert(ofs.NumFilesystems(), qt.Equals, 0)
	_, err := ofs.Stat("mydir/notfound.txt")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(func() { ofs.Create("mydir/foo.txt") }, qt.PanicMatches, "overlayfs: there are no filesystems to write to")

	ofs = ofs.Append(basicFs("1", "1"))
	c.Assert(ofs.NumFilesystems(), qt.Equals, 1)
	_, err = ofs.Stat("mydir/f1-1.txt")
	c.Assert(err, qt.IsNil)
	f, err := ofs.Create("mydir/foo.txt")
	c.Assert(err, qt.IsNil)
	f.Close()
}

func TestConcurrencyDir(t *testing.T) {
	c := qt.New(t)
	ofs1Fs := New(Options{Fss: []afero.Fs{basicFs("1", "1")}})
	ofs2Fs := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "1")}})
	const mydir = "mydir"

	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			for _, ofsFs := range []*OverlayFs{ofs1Fs, ofs2Fs} {
				d, err := ofsFs.Open(mydir)
				c.Assert(err, qt.IsNil)
				fi, err := d.Stat()
				c.Assert(err, qt.IsNil)
				c.Assert(fi.Name(), qt.Equals, mydir)
				c.Assert(d.Close(), qt.IsNil)
				fi, err = ofsFs.Stat(mydir)
				c.Assert(err, qt.IsNil)
				c.Assert(fi.Name(), qt.Equals, mydir)
				c.Assert(fi.IsDir(), qt.IsTrue)
			}
		})
	}

	wg.Wait()
}

func TestConcurrencyDirEmptyFs(t *testing.T) {
	c := qt.New(t)
	emptyFs := New(Options{})
	const mydir = "mydir"

	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			d, err := emptyFs.Open(mydir)
			c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
			c.Assert(d, qt.IsNil)
			fi, err := emptyFs.Stat(mydir)
			c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
			c.Assert(fi, qt.IsNil)
		})
	}

	wg.Wait()
}

func TestFileystemIterator(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("", "1"), basicFs("", "2")
	ofs := New(Options{Fss: []afero.Fs{fs1, fs2}})

	c.Assert(ofs.NumFilesystems(), qt.Equals, 2)
	c.Assert(ofs.Filesystem(0), qt.Equals, fs1)
	c.Assert(ofs.Filesystem(1), qt.Equals, fs2)
	c.Assert(ofs.Filesystem(2), qt.IsNil)
}

func TestOpenDir(t *testing.T) {
	c := qt.New(t)
	fs1, fs2, fs3 := basicFs("1", "1"), basicFs("1", "2"), basicFs("2", "2")
	fi1, _ := fs1.Stat("mydir")
	info := func() (os.FileInfo, error) { return fi1, nil }
	dir, err := OpenDir(
		nil,
		info,
		func() (afero.File, error) {
			return fs1.Open("mydir")
		},
		func() (afero.File, error) {
			return fs2.Open("mydir")
		},
		func() (afero.File, error) {
			return fs3.Open("mydir")
		},
	)
	c.Assert(err, qt.IsNil)

	dirEntries, err := dir.ReadDir(-1)

	c.Assert(err, qt.IsNil)
	c.Assert(dirEntries, qt.HasLen, 4)
	fi, err := dir.Stat()
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "mydir")
	c.Assert(dir.Close(), qt.IsNil)
}

func TestReadOps(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("1", "1"), basicFs("2", "2")
	ofs := New(Options{Fss: []afero.Fs{fs1, fs2}})

	c.Assert(ofs.Name(), qt.Equals, "overlayfs")

	// Open
	c.Assert(readFile(c, ofs, "mydir/f1-1.txt"), qt.Equals, "f1-1")
	c.Assert(readFile(c, ofs, "mydir/f2-2.txt"), qt.Equals, "f2-2")

	// Stat
	fi, err := ofs.Stat("mydir/f1-1.txt")
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "f1-1.txt")
	_, err = ofs.Stat("mydir/notfound.txt")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	fi, err = ofs.Stat("mydir/f2-2.txt")
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "f2-2.txt")

	// LstatIfPossible
	fi, _, err = ofs.LstatIfPossible("mydir/f2-1.txt")
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "f2-1.txt")
	_, _, err = ofs.LstatIfPossible("mydir/notfound.txt")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

func TestReadOpsErrors(t *testing.T) {
	c := qt.New(t)
	statErr := errors.New("stat error")
	fs1, fs2 := basicFs("1", "1"), &testFs{statErr: statErr}
	ofs := New(Options{Fss: []afero.Fs{fs1, fs2}})

	fi, err := ofs.Stat("mydir/f1-1.txt")
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "f1-1.txt")
	_, err = ofs.Stat("mydir/notfound.txt")
	c.Assert(err, qt.ErrorIs, statErr)

	// LstatIfPossible
	fi, _, err = ofs.LstatIfPossible("mydir/f2-1.txt")
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "f2-1.txt")
	_, _, err = ofs.LstatIfPossible("mydir/notfound.txt")
	c.Assert(err, qt.ErrorIs, statErr)
}

func TestOpenRecursive(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("1", "1"), basicFs("1", "2")
	fs3, fs4 := basicFs("2", "3"), basicFs("1", "4")
	ofs2 := New(Options{Fss: []afero.Fs{fs1, fs2}})
	ofs3 := New(Options{Fss: []afero.Fs{ofs2, fs3, fs4}})
	ofs1 := New(Options{Fss: []afero.Fs{ofs3}})

	c.Assert(readFile(c, ofs1, "mydir/f1-1.txt"), qt.Equals, "f1-1")
	c.Assert(readFile(c, ofs1, "mydir/f1-2.txt"), qt.Equals, "f1-3")
}

func TestWriteOpsReadonly(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("1", "1"), basicFs("1", "2")
	ofsReadOnly := New(Options{Fss: []afero.Fs{fs1, fs2}})

	_, err := ofsReadOnly.Create("mydir/foo.txt")
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	_, err = ofsReadOnly.OpenFile("mydir/foo.txt", os.O_CREATE, 0o777)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Chmod("mydir/foo.txt", 0o666)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Chown("mydir/foo.txt", 1, 2)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Chtimes("mydir/foo.txt", time.Now(), time.Now())
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Mkdir("mydir", 0o777)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.MkdirAll("mydir", 0o777)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Remove("mydir")
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.RemoveAll("mydir")
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Rename("a", "b")
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)
}

func TestWriteOpsFirstWriteable(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("1", "1"), basicFs("1", "2")
	ofs := New(Options{Fss: []afero.Fs{fs1, fs2}, FirstWritable: true})

	f, err := ofs.Create("mydir/foo.txt")
	c.Assert(err, qt.IsNil)
	f.Close()
}

func TestReaddir(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("1", "1"), basicFs("1", "2")
	fs3, fs4 := basicFs("2", "3"), basicFs("1", "4")
	ofs2 := New(Options{Fss: []afero.Fs{fs1, fs2}})
	ofs1 := New(Options{Fss: []afero.Fs{ofs2, fs3, fs4}})

	dirnames := readDirnames(c, ofs1, "mydir")

	c.Assert(dirnames, qt.DeepEquals, []string{"f1-1.txt", "f2-1.txt", "f1-2.txt", "f2-2.txt"})

	ofsSingle := New(Options{Fss: []afero.Fs{basicFs("1", "1")}})

	dirnames = readDirnames(c, ofsSingle, "mydir")

	c.Assert(dirnames, qt.DeepEquals, []string{"f1-1.txt", "f2-1.txt"})
}

func TestReaddirN(t *testing.T) {
	c := qt.New(t)
	// 6 files.
	ofs := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "2"), basicFs("3", "3")}})

	d, _ := ofs.Open("mydir")

	for range 3 {
		fis, err := d.Readdir(2)
		c.Assert(err, qt.IsNil)
		c.Assert(len(fis), qt.Equals, 2)
	}

	_, err := d.Readdir(1)
	c.Assert(err, qt.ErrorIs, io.EOF)
	c.Assert(d.Close(), qt.IsNil)

	d, _ = ofs.Open("mydir")
	fis, err := d.Readdir(32)
	c.Assert(err, qt.IsNil)
	c.Assert(len(fis), qt.Equals, 6)
	fis, err = d.Readdir(-1)
	c.Assert(len(fis), qt.Equals, 0)
	c.Assert(err, qt.ErrorIs, io.EOF)
	c.Assert(d.Close(), qt.IsNil)

	d, _ = ofs.Open("mydir")
	fis, err = d.Readdir(1)
	c.Assert(err, qt.IsNil)
	c.Assert(len(fis), qt.Equals, 1)
	fis, err = d.Readdir(4)
	c.Assert(len(fis), qt.Equals, 4)
	c.Assert(err, qt.IsNil)
	c.Assert(d.Close(), qt.IsNil)

	d, _ = ofs.Open("mydir")
	dirnames, err := d.Readdirnames(3)
	c.Assert(err, qt.IsNil)
	c.Assert(dirnames, qt.DeepEquals, []string{"f1-1.txt", "f2-1.txt", "f1-2.txt"})
	c.Assert(d.Close(), qt.IsNil)

	d, _ = ofs.Open("mydir")
	_, err = d.Readdir(-1)
	c.Assert(err, qt.IsNil)
	_, err = d.Readdir(-1)
	c.Assert(err, qt.ErrorIs, io.EOF)
	c.Assert(d.Close(), qt.IsNil)
}

func TestReaddirStable(t *testing.T) {
	c := qt.New(t)

	// 6 files.
	ofs := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "2"), basicFs("3", "3")}})
	d, _ := ofs.Open("mydir")
	fis1, err := d.Readdir(-1)
	c.Assert(err, qt.IsNil)
	d.Close()
	d, _ = ofs.Open("mydir")
	fis2, err := d.Readdir(2)
	c.Assert(err, qt.IsNil)
	c.Assert(d.Close(), qt.IsNil)
	c.Assert(fis1[0].Name(), qt.Equals, "f1-1.txt")
	c.Assert(fis2[0].Name(), qt.Equals, "f1-1.txt")
	sort.Slice(fis1, func(i, j int) bool { return fis1[i].Name() > fis1[j].Name() })
	sort.Slice(fis2, func(i, j int) bool { return fis2[i].Name() > fis2[j].Name() })
	checkFi := func() {
		c.Assert(fis1[0].Name(), qt.Equals, "f2-3.txt")
		c.Assert(fis2[0].Name(), qt.Equals, "f2-1.txt")
	}
	checkFi()
	for range 10 {
		d, _ = ofs.Open("mydir")
		d.Readdir(-1)
		c.Assert(d.Close(), qt.IsNil)
	}
	checkFi()
}

func TestReadDir(t *testing.T) {
	c := qt.New(t)
	// 6 files.
	ofs := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "2"), basicFs("3", "3")}})

	d, _ := ofs.Open("mydir")

	dirEntries, err := d.(fs.ReadDirFile).ReadDir(-1)
	c.Assert(err, qt.IsNil)
	c.Assert(len(dirEntries), qt.Equals, 6)
	c.Assert(dirEntries[0].Name(), qt.Equals, "f1-1.txt")
}

func TestDirOps(t *testing.T) {
	c := qt.New(t)
	ofs := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "1")}})

	dir, err := ofs.Open("mydir")
	c.Assert(err, qt.IsNil)

	c.Assert(dir.Name(), qt.Equals, "mydir")
	_, err = dir.Stat()
	c.Assert(err, qt.IsNil)

	// operation not supported on.*.
	c.Assert(dir.Sync, qt.PanicMatches, `operation not supported on.*`)

	c.Assert(func() { dir.Truncate(0) }, qt.PanicMatches, `operation not supported on.*`)
	c.Assert(func() { dir.WriteString("asdf") }, qt.PanicMatches, `operation not supported on.*`)
	c.Assert(func() { dir.Write(nil) }, qt.PanicMatches, `operation not supported on.*`)
	c.Assert(func() { dir.WriteAt(nil, 21) }, qt.PanicMatches, `operation not supported on.*`)
	c.Assert(func() { dir.Read(nil) }, qt.PanicMatches, `operation not supported on.*`)
	c.Assert(func() { dir.ReadAt(nil, 21) }, qt.PanicMatches, `operation not supported on.*`)
	c.Assert(func() { dir.Seek(1, 2) }, qt.PanicMatches, `operation not supported on.*`)

	c.Assert(dir.Close(), qt.IsNil)
	_, err = dir.Stat()
	c.Assert(err, qt.ErrorIs, fs.ErrClosed)
}

func readDirnames(c *qt.C, fs afero.Fs, name string) []string {
	dir, err := fs.Open(name)
	c.Assert(err, qt.IsNil)
	defer dir.Close()

	dirnames, err := dir.Readdirnames(-1)
	c.Assert(err, qt.IsNil)
	return dirnames
}

func readFile(c *qt.C, fs afero.Fs, name string) string {
	c.Helper()
	f, err := fs.Open(name)
	c.Assert(err, qt.IsNil)
	defer f.Close()
	b, err := afero.ReadAll(f)
	c.Assert(err, qt.IsNil)
	return string(b)
}

func basicFs(idFilename, idContent string) afero.Fs {
	return fsFromTxtTar(
		strings.ReplaceAll(
			strings.ReplaceAll(`
-- mydir/f1-IDFILENAME.txt --
f1-IDCONTENT
-- mydir/f2-IDFILENAME.txt --
f2-IDCONTENT
`, "IDCONTENT", idContent), "IDFILENAME", idFilename))
}

func fsFromTxtTar(s string) afero.Fs {
	data := txtar.Parse([]byte(s))
	fs := afero.NewMemMapFs()
	for _, f := range data.Files {
		if err := afero.WriteFile(fs, f.Name, bytes.TrimSuffix(f.Data, []byte("\n")), 0o666); err != nil {
			panic(err)
		}
	}
	return fs
}

type testFs struct {
	statErr error
}

func (fs *testFs) Stat(name string) (os.FileInfo, error) {
	return nil, fs.statErr
}

func (fs *testFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	return nil, false, fs.statErr
}

func (fs *testFs) Name() string {
	return "testFs"
}

func (fs *testFs) Create(name string) (afero.File, error) {
	panic("not implemented")
}

func (fs *testFs) Mkdir(name string, perm os.FileMode) error {
	panic("not implemented")
}

func (fs *testFs) MkdirAll(path string, perm os.FileMode) error {
	panic("not implemented")
}

func (fs *testFs) Open(name string) (afero.File, error) {
	panic("not implemented")
}

func (fs *testFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	panic("not implemented")
}

func (fs *testFs) Remove(name string) error {
	panic("not implemented")
}

func (fs *testFs) RemoveAll(path string) error {
	panic("not implemented")
}

func (fs *testFs) Rename(oldname string, newname string) error {
	panic("not implemented")
}

func (fs *testFs) Chmod(name string, mode os.FileMode) error {
	panic("not implemented")
}

func (fs *testFs) Chown(name string, uid int, gid int) error {
	panic("not implemented")
}

func (fs *testFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	panic("not implemented")
}

func BenchmarkOverlayFs(b *testing.B) {
	createFs := func(dir, fileID string, numFiles int) afero.Fs {
		fs := afero.NewMemMapFs()
		for i := range numFiles {
			if err := afero.WriteFile(fs, filepath.Join(dir, fmt.Sprintf("f%s-%d.txt", fileID, i)), []byte("foo"), 0o666); err != nil {
				b.Fatal(err)
			}
		}
		return fs
	}
	fs1, fs2, fs3 := createFs("mydir", "1", 10), createFs("mydir", "2", 10), createFs("mydir", "3", 10)
	fs4, fs5 := createFs("mydir", "1", 4), createFs("myotherdir", "1", 4)
	ofs := New(Options{FirstWritable: true, Fss: []afero.Fs{fs1, fs2, fs3, fs4, fs5}})

	runBenchMark := func(name string, fn func(b *testing.B)) {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				fn(b)
			}
		})
	}

	runBenchMark("Stat", func(b *testing.B) {
		_, err := ofs.Stat("mydir/f2-2.txt")
		if err != nil {
			b.Fatal(err)
		}
	})

	runBenchMark("Open file", func(b *testing.B) {
		f, err := ofs.Open("mydir/f2-2.txt")
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	})

	runBenchMark("Open dir", func(b *testing.B) {
		f, err := ofs.Open("mydir")
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	})

	runBenchMark("Readdir all", func(b *testing.B) {
		f, err := ofs.Open("mydir")
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.Readdir(-1)
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	})

	runBenchMark("Readdir in one fs all", func(b *testing.B) {
		f, err := ofs.Open("myotherdir")
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.Readdir(-1)
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	})

	runBenchMark("Readdir some", func(b *testing.B) {
		f, err := ofs.Open("mydir")
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.Readdir(2)
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	})

	runBenchMark("Readdir in one fs some", func(b *testing.B) {
		f, err := ofs.Open("myotherdir")
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.Readdir(2)
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	})
}
