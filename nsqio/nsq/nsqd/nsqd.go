package nsqd

import (
    "bytes"
    "crypto/tls"
    "crypto/x509"
    "encoding/json"
    "errors"
    "fmt"
    "io/ioutil"
    "log"
    "math/rand"
    "net"
    "os"
    "path"
    "runtime"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/nsqio/nsq/internal/clusterinfo"
    "github.com/nsqio/nsq/internal/dirlock"
    "github.com/nsqio/nsq/internal/http_api"
    "github.com/nsqio/nsq/internal/lg"
    "github.com/nsqio/nsq/internal/protocol"
    "github.com/nsqio/nsq/internal/statsd"
    "github.com/nsqio/nsq/internal/util"
    "github.com/nsqio/nsq/internal/version"
)

const (
    TLSNotRequired = iota
    TLSRequiredExceptHTTP
    TLSRequired
)

type errStore struct {
    err error
}

