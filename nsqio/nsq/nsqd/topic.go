package nsqd

import (
    "bytes"
    "errors"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/nsqio/go-diskqueue"
    "github.com/nsqio/nsq/internal/lg"
    "github.com/nsqio/nsq/internal/quantile"
    "github.com/nsqio/nsq/internal/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
    messageCount uint64

    sync.RWMutex

    name              string
    channelMap        map[string]*Channel
    backend           BackendQueue
    memoryMsgChan     chan *Message
    exitChan          chan int
    channelUpdateChan chan int
    waitGroup         util.WaitGroupWrapper
    exitFlag          int32
    idFactory         *guidFactory

    ephemeral      bool
    deleteCallback func(*Topic)
    deleter        sync.Once

    paused      int32
    pausedChan  chan bool

    ctx *context
}

// Topic constructor
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
    t := &Topic{
        name:              topicName,
        channelMap:         make(map[string]*Channel),
        memoryMsgChan:      make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
        exitChan:       make(chan int),
        channelUpdateChan: make(chan int),
        ctx:        ctx,
        pauseChan:  make(chan bool),
        deleteCallback: deleteCallback,
        idFactory: NewGUIDFactory(ctx.nsqd.getOpts().ID),
    }

    if strings.HasSuffix(topicName, "#ephemeral") {
        t.ephemeral = true
        t.backend = newDummyBackendQueue
    } else {
        dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
             opts := ctx.nsqd.getOpts()
             lg.Logf(opts.Logger, opts.logLevel, lg.LogLevel(level), f, args...)
         }
         t.backend = diskqueue.New(
             topicName,
             ctx.nsqd.getOpts().DataPath,
             ctx.nsqd.getOpts().MaxBytesPerFile,
             int32(minValidMsgLength),
             int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
             ctx.nsqd.getOpts().SyncEvery,
             ctx.nsqd.getOpts().SyncTimeout,
             dqLogf,
         )
     }

     t.WaitGroupWrapper(func() { t.messagePump() })

     t.ctx.nsqd.Notify(t)

     return t
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
    return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
    t.Lock()
    channel, isNew := t.getOrCreateChannel(channelName)
    t.Unlock()

    if isNew {
        // update messagePump state
        select {
        case t.channelUpdateChan <- 1:
        case <-t.exitChan:
        }
    }

    return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
    channel, ok := t.channelMap[channelName]
    if !ok {
        deleteCallback := func(c *Channel) {
            t.DeleteExistingChannel(c.name)
        }
        channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
        t.channelMap[channelMap] = channel
        t.ctc.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
        return channel, true
    }
    return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
    t.RLock()
    defer t.RUnlock()
    channel, ok := t.channelMap[channelName]
    if !ok {
        return nil, errors.New("channel does not exist")
    }
    return channel, nil
}

//  DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
    t.Lock()
    channel, ok := t.channelMap[channelName]
    if !ok {
        t.Unlock()
        return errors.New("channel does not exist")
    }
    delete(t.channelMap, channelName)
    // not defered so that we can continue while the channel async closes
    numChannels := len(t.channelMap)
    t,Unlock()

    t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

    // delete empties the channel before closing
    // (so that we dont leave any messages around)
    channel.Delete()

    // update messagePump state
    select {
    case t.channelUpdateChan <- 1:
    case <-t.exitChan:
    }

    if numChannels == 0 && t.ephemeral == true {
        go t.deleter.Do(func() { t.deleteCallback(t) })
    }

    return nil
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
    t.RLock()
    defer t.RUnlock()
    if atomic.LoadInt32(&t.exitFlag) == 1 {
        return errors.New("exiting")
    }
    err := t.put(m)
    if err != nil {
        return err
    }
    atomic.AddUint64(&t.messageCount, 1)
    return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
    t.RLock()
    defer t.RUnlock()
    if atomic.LoadInt32(&t.exitFlag) == 1 {
        return errors.New("exiting")
    }

    for _, m := range msgs {
        err := t.put(m)
        if err != nil {
            return err
        }
    }
    atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
    return nil
}

func (t *Topic) put(m *Message) error {
    select {
    case t.memoryMsgChan <- m:
    default:
        b := bufferPoolGet()
        err := writeMessageToBackend(b, m, t.backend)
        bufferPoolPut(b)
        t.ctx.nsqd.SetHealth(err)
        if err != nil {
            t.ctx.nsqd.logf(LOG_ERROR,
                "TOPIC(%s) ERROR: failed to write message to backend - %s",
                t.name, err)
            return err
        }
    }
    return nil
}

func (t *Topic) Depth() int64 {
    return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and 
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
    var msg *Message
    var buf []byte
    var err error
    var chans []*Channel
    var memoryMsgChan chan *Message
    var backendChan chan []byte

    t.RLock()
    for _, c := range t.channelMap {
       channs = append(chans, c)
    }
    t.RUnlock()

    if len(chans) > 0 {
        memoryMsgChan = t.memoryMsgChan
        backendChan = t.backend.ReadChan()
    }

    for {
        select {
        case msg = <-memoryMsgChan:
        case buf = <-backendChan:
            msg, err = decodeMessage(buf)
            if err != nil {
                t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
                continue
            }
        case <-t.channelUpdateChan:
            chans = chans[:0]
            t.RLock()
            for _, c := range t.channelMap {
                chans = append(chans, c)
            }
            c.RUnlock()
            if len(chans) == 0 || t.IsPaused() {
                memoryMsgChan = nil
                backendChan = nil
            } else {
                memoryMsgChan = t.memoryMsgChan
                backendChan = t.backend.ReadChan()
            }
            continue
        case pause := <-t.pauseChan:
            if pause || len(chans) == 0 {
                memoryMsgChan = nil
                backendChan = nil
            } else {
                memoryMsgChan = t.memoryMsgChan
                backendChan = t.backend.ReadChan()
            }
            continue
        case <-t.exitChan:
            goto exit
        }

        for i, channel := range chans {
            chanMsg := msg
            // copy the message because each channel
            // needs a unique instance but...
            // fastpath to avoid copy if its the first channel
            // (the topic already created the first copy)
            if i > 0 {
                chanMsg = NewMessage(msg.ID, msg.Body)
                chanMsg.Timestamp = msg.Timestamp
                chanMsg.deferred = msg.deferred
            }
            if chanMsg.deferred != 0 {
                channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
                continue
            }
            err := channel.PutMessage(chanMsg)
            if err != nil {
                t.ctx.nsqd.logf(LOG_ERROR,
                "TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
                t.name, msg.ID, channel.name, err)
            }
        }
    }

exit:
    t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ...messagePump", t.name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
    return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
    return t.exit(false)
}




