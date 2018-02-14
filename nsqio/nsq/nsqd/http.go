package nsqd

import (
    "bufio"
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "io/ioutil"
    "net/http"
    "net/http/pprof"
    "net/url"
    "os"
    "reflect"
    "runtime"
    "strconv"
    "strings"
    "time"

    "github.com/julienschmidt/httprouter"
    "github.com/nsqio/nsq/internal/http_api"
    "github.com/nsqio/nsq/internal/lg"
    "github.com/nsqio/nsq/internal/protocol"
    "github.com/nsqio/nsq/internal/version"
)

var boolParams = map[string]bool{
    "true":  true,
    "1":     true,
    "false": false,
    "0":     false,
}

type httpServer struct{
    ctx         *context
    tlsEnabled  bool
    tlsRequired bool
    router      http.Handler
}

func newHTTPServer(ctx *context, tlsEnabled bool, tlsRequired bool) *httpServer {
    log := http_api.Log(ctx.nsqd.logf)

    router := httprouter.New()
    router.HandleMethodNotAllowed = true
    router.PanicHandler = http_api.LogPanicHandler(ctx.nsqd.logf)
    router.NotFound = http_api.LogNotFoundHandler(ctx.nsqd.logf)
    router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqd.logf)
}
