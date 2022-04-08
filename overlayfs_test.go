package overlayfs

import (
	"bytes"
	"io/fs"
	"os"
	"strings"
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

func TestFileystemIterator(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("", "1"), basicFs("", "2")
	ofs := New(Options{Fss: []afero.Fs{fs1, fs2}})

	c.Assert(ofs.NumFilesystems(), qt.Equals, 2)
	c.Assert(ofs.Filesystem(0), qt.Equals, fs1)
	c.Assert(ofs.Filesystem(1), qt.Equals, fs2)
	c.Assert(ofs.Filesystem(2), qt.IsNil)
}

func TestReadOps(t *testing.T) {
	c := qt.New(t)
	fs1, fs2 := basicFs("1", "1"), basicFs("1", "2")
	ofs := New(Options{Fss: []afero.Fs{fs1, fs2}})

	c.Assert(ofs.Name(), qt.Equals, "overlayfs")

	// Open
	c.Assert(readFile(c, ofs, "mydir/f1-1.txt"), qt.Equals, "f1-1")

	// Stat
	fi, err := ofs.Stat("mydir/f1-1.txt")
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "f1-1.txt")
	_, err = ofs.Stat("mydir/notfound.txt")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)

	// LstatIfPossible
	fi, _, err = ofs.LstatIfPossible("mydir/f2-1.txt")
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, "f2-1.txt")
	_, _, err = ofs.LstatIfPossible("mydir/notfound.txt")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)

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

	_, err = ofsReadOnly.OpenFile("mydir/foo.txt", os.O_CREATE, 0777)

	err = ofsReadOnly.Chmod("mydir/foo.txt", 0666)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Chown("mydir/foo.txt", 1, 2)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Chtimes("mydir/foo.txt", time.Now(), time.Now())
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.Mkdir("mydir", 0777)
	c.Assert(err, qt.ErrorIs, fs.ErrPermission)

	err = ofsReadOnly.MkdirAll("mydir", 0777)
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

func TestReadDir(t *testing.T) {
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

func TestDirOps(t *testing.T) {
	c := qt.New(t)
	ofs := New(Options{Fss: []afero.Fs{basicFs("1", "1"), basicFs("2", "1")}})

	dir, err := ofs.Open("mydir")
	c.Assert(err, qt.IsNil)

	c.Assert(dir.Name(), qt.Equals, "mydir")
	c.Assert(dir.Close(), qt.IsNil)
	fi, err := dir.Stat()
	c.Assert(err, qt.IsNil)
	c.Assert(fi.Name(), qt.Equals, `mydir`)

	// Not implemented.
	c.Assert(func() { dir.Readdir(21) }, qt.PanicMatches, `.*not implemented`)
	c.Assert(func() { dir.Readdirnames(21) }, qt.PanicMatches, `.*not implemented`)

	// Not supported.
	c.Assert(dir.Sync, qt.PanicMatches, `not supported`)

	c.Assert(func() { dir.Truncate(0) }, qt.PanicMatches, `not supported`)
	c.Assert(func() { dir.WriteString("asdf") }, qt.PanicMatches, `not supported`)
	c.Assert(func() { dir.Write(nil) }, qt.PanicMatches, `not supported`)
	c.Assert(func() { dir.WriteAt(nil, 21) }, qt.PanicMatches, `not supported`)
	c.Assert(func() { dir.Read(nil) }, qt.PanicMatches, `not supported`)
	c.Assert(func() { dir.ReadAt(nil, 21) }, qt.PanicMatches, `not supported`)
	c.Assert(func() { dir.Seek(1, 2) }, qt.PanicMatches, `not supported`)
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
		if err := afero.WriteFile(fs, f.Name, bytes.TrimSuffix(f.Data, []byte("\n")), 0666); err != nil {
			panic(err)
		}

	}
	return fs
}
