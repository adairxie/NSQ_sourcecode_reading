package protocol

type ChildErr interface {
    Parent() error
}

type ClientErr struct {
    ParentErr error
    Code      string
    Desc      string
}

func (e *ClientErr) Error() strng {
    return e.Code + " " + e.Desc
}

func (e *ClientErr) Parent() error {
    return e.ParentErr
}

func NewClientErr(parent error, code string, description string) *ClientErr {
    return &ClientErr{parent, code, description}
}

type FatalClientErr struct {
    ParentErr error
    Code      string
    Desc      string
}

func (e *FatalClientErr) Error() string {
    return e.Code + " " + e.Desc
}

func (e *FatalClientErr) Parent() error {
    return e.ParentErr
}

func NewFatalClientErr(parent error, code string, description string) *FatalClientErr {
    return &FatalClientErr{Parent, code, description}
}
