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
    s := &httpServer{
        ctx:         ctx,
        tlsEnabled:  tlsEnabled,
        tlsRequired: tlsRequired,
        router:      router,
    }

    router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
    router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

    // v1 negotiate
    router.Handle("POST", "/pub", http_api.Decorate(s.doPUB, http_api.V1))
    router.Handle("POST", "/mpub", http_api.Decorate(s.doMPUB, http_api.V1))
    router.Handle("GET", "/stats", http_api.Decorate(s.doStats, log, http_api.V1))

    // only v1
    router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
    router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
    router.Handle("POST", "/topic/empty", http_api.Decorate(s.doEmptyTopic, log, http_api.V1))
    router.Handle("POST", "/topic/pause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))
    router.Handle("POST", "/topic/unpause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))
    router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
    router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
    router.Handle("POST", "/channel/empty", http_api.Decorate(s.doEmptyChannel, log, http_api.V1))
    router.Handle("POST", "/channel/pause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
    router.Handle("POST", "/channel/unpause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
    router.Handle("GET", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))
    router.Handle("PUT", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))

    // debug
}
