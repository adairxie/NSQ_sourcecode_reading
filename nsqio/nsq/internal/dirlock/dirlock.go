

package dirlock

import (
    "fmt"
    "os"
    "syscall"
)

type Dirlock struct {
    dir string
    f   *os.File
}

func New(dir string) *Dirlock {
    return &Dirlock{
        dir: dir,
    }
}

func (l *Dirlock) Lock() error {
    f, err := os.Open(l.dir)
    if err != nil {
        return err
    }
    l.f = f
    err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
    if err != nil {
        return fmt.Errorf("cannot flock directory %s - %s", l.dir, err)
    }
    return nil
}

func (l *Dirlock) Unlock() error {
    defer l.f.Close()
    return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}

