package http_api

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "time"

    "github.com/julienschmidt/httprouter"
    "github.com/nsqio/nsq/internal/lg"
)

type Decorator func(APIHandler) APIHandler

type APIHandler func(http.ResponseWriter, *http.Request, httprouter.Params) (interface{}, error)

type Err struct {
    Code int
    Text string
}

func (e Err) Error() string {
    return e.Text
}

func acceptVersion(req *http.Request) int {
    if req.Header.Get("accept") == "application/vnd.nsq; version=1.0" {
        return 1
    }

    return 0
}

func PlainText(f APIHandler) APIHandler {
    return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
        code := 200
        data, err := f(w, req, ps)
        if err != nil {
            code = err.(Err).Code
            data = err.Error()
        }
        switch d := data.(type) {
        case string:
            w.WriteHeader(code)
            io.WriteString(w, d)
        case []byte:
            w.WriteHeader(code)
            w.Write(d)
        default:
            panic(fmt.Sprintf("unknowm response type %T", data))
        }
        return nil, nil
    }
}

func V1(f APIHandler) APIHandler {
    return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (internal{}, error) {
        data, err := f(w, req, ps)
        if err != nil {
            RespondV1(w, err.(Err).Code, err)
            return nil, nil
        }
        RespondV1(w, 200, data)
        return nil, nil
    }
}

func RespondV1(w http.ResponseWriter, code int, data interface{}) {
    var response []byte
    var err error
    var isJSON bool

    if code == 200 {
        switch data.(type) {
        case string:
            response = []byte(data.(string))
        case []byte:
            response = data.([]byte)
        case nil:
            response = []byte{}
        default:
            isJSON = true
            response, err = json.Marshal(data)
            if err != nil {
                code = 500
                data = err
            }
        }
    }

    if code != 200 {
        isJSON = true
        response = []byte(fmt.Sprintf(`{"message":"%s"}`, data))
    }

    if isJSON {
        w.Header().Set("Content-Type", "application/json; charset=utf-8")
    }
    w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
    w.WriteHeader(code)
    w.Write(response)
}
