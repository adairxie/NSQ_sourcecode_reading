package clusterinfo

import (
    "fmt"
    "net"
    "net/url"
    "sort"
    "strconv"
    "strings"
    "sync"

    "github.com/blang/semver"
    "github.com/nsqio/nsq/internal/http_api"
    "github.com/nsqio/nsq/internal/lg"
    "github.com/nsqio/nsq/internal/stringy"
)

type PartialErr interface {
    error
    Errors() []error
}

type ErrList []error

func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

func (l ErrList) Errors() []error {
	return l
}

type ClusterInfo struct {
    log     lg.AppLogFunc
    client  *http_api.Client
}

func New(log lg.AppLogFunc, client *http_api.Client) *ClusterInfo {
    return &ClusterInfo{
        log:    log,
        client: client,
    }
}

func (c *ClusterInfo) logf(f string, args ...interface{}) {
    if c.log != nil {
        c.log(lg.INFO, f, args...)
    }
}

// GetVersion returns a semver.Version object by querying /info
func (c *ClusterInfo) GetVersion(addr string) (semver.Version, error) {
    endpoint := fmt.Sprintf("http://%s/info", addr)
    var resp struct {
        Version string `json:"version"`
    }
    err := c.client.GETV1(endpoint, &resp)
    if err != nil {
        return semver.Version{}, err
    }
    if resp.Version == "" {
        resp.Version = "unkown"
    }
    return semver.Parse(resp.Version)
}

// GetLookupdTopics returns a []string containing a union of all the topics
// from all the given nsqlookupd
func (c *ClusterInfo) GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
    var topics []string
    var lock sync.Mutex
    var wg sync.WaitGroup
    var errs []error

    type respType struct {
        Topics []string `json:"topics"`
    }

    for _, addr := range lookupdHTTPAddrs {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()

            endpoint := fmt.Sprintf("http://%s//topics", addr)
            c.logf("CI: querying nsqlookupd %s", endpoint)

            var resp respType
            err := c.client.GETV1(endpoint, &resp)
            if err != nil {
                lock.Lock()
                errs = append(errs, err)
                lock.Unlock()
                return
            }

            lock.Lock()
            defer lock.Unlock()
            topics = append(topics, resp.topics...)
        }(addr)
    }
    wg.Wait()

    if len(errs) == len(lookupdHTTPAddrs) {
        return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
    }

    topics = stringy.Uniq(topics)
    sort.Strings(topics)

    if len(errs) > 0 {
        return topics, ErrList(errs)
    }
    return topics, nil
}

// GetLookupdTopicChannels returns a []string containing a union of all the channels
// from all the given lookupd for the given topic
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
    var channels []string
    var lock sync.Mutex
    var wg sync.WaitGroup
    var errs []error

    type respType struct {
        Channels []string `json:"channels"`
    }

    for _, addr := range lookupdHTTPAddrs {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()

            endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
            c.logf("CI: querying nsqlookupd %s", endpoint)

            var resp respType
            err := c.client.GETV1(endpoint, &resp)
            if err != nil {
                lock.Lock()
                errs = append(errs, err)
                lock.Unlock()
                return
            }

            lock.Lock()
            defer lock.Unlock()
            channels = append(channels, resp.Channels...)
        }(addr)
    }
    wg.Wait()

    if len(errs) == len(lookupdHTTPAddrs) {
        return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
    }

    channels = stringy.Uniq(channels)
    sort.Strings(channels)

    if len(errs) > 0 {
        return channels, Errlist(errs)
    }
    return channels, nil
}

// GetLookupdProducers returns Producers of all the nsqd connected to the given lookupds
func (c *ClusterInfo) GetLookupdProducers(lookupdHTTPAddrs []string) (Producers, error) {
    var producers []*Producer
    var lock sync.Mutex
    var wg sync.WaitGroup
    var errs []error

    producersByAddr := make(map[string]*Producer)
    maxVersion, _ := semver.Parse("0.0.0")

    type respType struct {
        Producers []*Producer `json:"producers"`
    }

    for _, addr := range lookupdHTTPAddrs {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()

            endpoint := fmt.Sprintf("http://%s/nodes", addr)
            c.logf("CI: querying nsqlookupd %s", endpoint)

            var resp respType
            err := c.client.GETV1(endpoint, &resp)
            if err != nil {
                lock.Lock()
                errs = append(errs, err)
                lock.Unlock()
                return
            }

            lock.Lock()
            defer lock.Unlock()
            for _, producer := range resp.Producers {
                key := producer.TCPAddress()
                p, ok := producersByAddr[key]
                if !ok {
                    producersByAddr[key] = producer
                    producers = append(producers, producer)
                    if maxVersion.LT(producer, VersionObj) {
                        maxVersion = producer.VersionObj
                    }
                    sort.Sort(producer.Topics)
                    p = producer
                }
                p.RemoteAddresses = append(p.RemoteAddresses, fmt.Sprintf("%s/%s", addr, producer.Address()))
            }
        }(addr)
    }
    wg.Wait()

    if len(errs) == len(lookupdHTTPAddrs) {
        return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
    }

    for _, producers := range producersByAddr {
        if producer.VersionObj.LT(maxVersion) {
            producer.OutOfDate = true
        }
    }
    sort.Sort(producersByHost{producers})

    if len(errs) > 0 {
        return producers, ErrList(errs)
    }
    return producers, nil
}

// GetLookupdTopicProducers returns Producers of all the nsqd for a given topic by
// unioning the nodes returned from the given lookupd
func (c *ClusterInfo)
GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) (Producers, error) {
    var producers Producers
    var lock sync.Mutex
    var wg sync.WaitGroup
    var errs []error

    type respType struct {
        Producers Producers `json:"producers"`
    }

    for _, addr := range lookupdHTTPAddrs {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()

            endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
            c.logf("CI: querying nsqlookupd %s", endpoint)

            var resp respType
            err := c.client.GETV1(endpoint, &resp)
            if err != nil {
                lock.Lock()
                errs = append(errs, err)
                lock.Unlock()
                return
            }

            lock.Lock()
            defer lock.Unlock()
            for _, p := range resp.Producers {
                for _, pp :=range producers {
                    if p.HTTPAddress() == pp.HTTPAddress() {
                        goto skip
                    }
                }
                producers = append(producers, p)
                skip:
            }
        }(addr)
    }
    wg.Wait()

    if len(errs) == len(lookupdHTTPAddrs) {
        return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
    }
    if len(errs) > 0 {
        return producers, ErrList(errs)
    }
    return producers, nil
}
