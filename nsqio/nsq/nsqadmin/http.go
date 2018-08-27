package nsqadmin

import (
    "encoding/json"
    "fmt"
    "html/template"
    "io"
    "io/ioutil"
    "mime"
    "net"
    "net/http"
    "net/http/httputil"
    "net/url"
    "path"
    "reflect"
    "strings"
    "time"

    "github.com/julienschmidt/httprouter"
    "github.com/nsqio/nsq/internal/clusterinfo"
    "github.com/nsqio/nsq/internal/http_api"
    "github.com/nsqio/nsq/internal/lg"
    "github.com/nsqio/nsq/internal/protocol"
    "github.com/nsqio/nsq/internal/version"
}

func maybeWarnMsg(msgs []string) string {
    if len(msgs) > 0 {
        return "WARNING: " + strings.Join(msgs, "; ")
    }
    return ""
}

// this is similar to httputil.NewSingleHostReverseProxy except it passes along basic auth
func NewSingleHostReverseProxy(target *url.URL, connectTimeout time.Duration, requestTimeout time.Duration) *httputil.ReverseProxy {
    director := func(req *http.Request) {
        req.URL.Scheme = target.Scheme
        req.URL.Host = target.Host
        if target.User != nil {
            passwd, _ := target.User.Password()
            req.SetBasicAuth(target.User.Username(), passwd)
        }
    }
    return &httputil.ReverseProxy{
        Director: director,
        Transport: http_api.NewDeadlineTransport(connectTimeout, requestTimeout),
    }
}

type httpServer struct {
    ctx    *Context
    router http.Handler
    client *http_api.Client
    ci     *clusterinfo.ClusterInfo
}
