package sphinx

import (
    "bytes"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "net"
    "encoding/binary"
)

const (
    maxReqs            = 32
    connectTimeoutMsec = 1000
    maxPacketLen       = 8 * 1024 * 1024
)

type filter struct {
    attr        string
    filter_type int
    values      []int64
    umin        int64
    umax        int64
    fmin        float
    fmax        float
    exclude     int
}

type override struct {
    attr        string
    docids      []uint64
    uint_values []uint
}

type Client struct {
    ver_search int
    error      string
    warning    string

    host    string
    port    int
    timeout float
    offset  int
    limit   int

    mode                 int
    num_weights          int
    weights              []int
    sort                 int
    sortby               string
    minid                uint64
    maxid                uint64
    group_by             string
    group_func           int
    group_sort           string
    group_distinct       string
    max_matches          int
    cutoff               int
    retry_count          int
    retry_delay          int
    geoanchor_attr_lat   string
    geoanchor_attr_long  string
    geoanchor_lat        float
    geoanchor_long       float
    num_filters          int
    max_filters          int
    filters              []filter
    num_index_weights    int
    index_weights_names  []string
    index_weights_values []int
    ranker               int
    max_query_time       int
    num_field_weights    int
    field_weights_names  []string
    field_weights_values []int
    num_overrides        int
    max_overrides        int
    overrides            []override
    select_list          string

    num_reqs int
    req_lens []int
    reqs     []string

    response_len int

    num_results int
}

const (
    searchdCommandSearch   = 0
    searchdCommandExcerpt  = 1
    searchdCommandUpdate   = 2
    searchdCommandKeywords = 3
    searchdCommandPersist  = 4
    searchdCommandStatus   = 5
)

const (
    verCommandExcerpt  = 0x100
    verCommandUpdate   = 0x101
    verCommandKeywords = 0x100
    verCommandStatus   = 0x100
)


/// known searchd status codes
const (
    SearchdOk      = 0
    SearchdError   = 1
    SearchdRetry   = 2
    SearchdWarning = 3
)

/// known match modes
const (
    SphMatchAll       = 0
    SphMatchAny       = 1
    SphMatchPhrase    = 2
    SphMatchBoolean   = 3
    SphMatchExtended  = 4
    SphMatchFullscan  = 5
    SphMatchExtended2 = 6
)

/// known ranking modes (ext2 only)
const (
    SphRankProximityBm25 = 0
    SphRankBm25          = 1
    SphRankNone          = 2
    SphRankWordCount     = 3
)

/// known sort modes
const (
    SphSortRelevance    = 0
    SphSortAttrDesc     = 1
    SphSortAttrAsc      = 2
    SphSortTimeSegments = 3
    SphSortExtended     = 4
    SphSortExpr         = 5
)

/// known filter types
const (
    SphFilterValues     = 0
    SphFilterRange      = 1
    SphFilterFloatRange = 2
)

/// known attribute types
const (
    SphAttrInteger   = 1
    SphAttrTimestamp = 2
    SphAttrOrdinal   = 3
    SphAttrBool      = 4
    SphAttrFloat     = 5
    SphAttrMulti     = 0x40000000
)

/// known grouping functions
const (
    SphGroupByDay      = 0
    SphGroupByWeek     = 1
    SphGroupByMonth    = 2
    SphGroupByYear     = 3
    SphGroupByAttr     = 4
    SphGroupByAttrPair = 5
)

// Sphinx errors.
type SphinxError struct {
    os.ErrorString
}

var (
    ErrConnect = &SphinxError{"connection error"}
)

func NewClient() (client *Client) {
    client = new(Client)

    client.ver_search = 0x116
    client.host = "localhost"
    client.port = 9312
    client.timeout = 0.0
    client.offset = 0
    client.limit = 20
    client.mode = SphMatchAll
    client.num_weights = 0
    client.weights = nil
    client.sort = SphSortRelevance
    client.sortby = ""
    client.minid = 0
    client.maxid = 0
    client.group_by = ""
    client.group_func = SphGroupByAttr
    client.group_sort = "@groupby desc"
    client.group_distinct = ""
    client.max_matches = 1000
    client.cutoff = 0
    client.retry_count = 0
    client.retry_delay = 0
    client.geoanchor_attr_lat = ""
    client.geoanchor_attr_long = ""
    client.geoanchor_lat = 0.0
    client.geoanchor_long = 0.0
    client.num_filters = 0
    client.max_filters = 0
    client.filters = nil
    client.num_index_weights = 0
    client.index_weights_names = nil
    client.index_weights_values = nil
    client.ranker = SphRankProximityBm25
    client.max_query_time = 0
    client.num_field_weights = 0
    client.field_weights_names = nil
    client.field_weights_values = nil
    client.num_overrides = 0
    client.max_overrides = 0
    client.overrides = nil
    client.select_list = ""

    client.num_reqs = 0
    client.response_len = 0
    // client.response_buf             = ""
    client.num_results = 0

    // for i := 0; i < maxReqs; i++ {
    // 	client.results[i].values_pool = nil
    // 	client.results[i].words = nil
    // 	client.results[i].fields = nil
    // 	client.results[i].attr_names = nil
    // 	client.results[i].attr_types = nil
    // }
    //
    // client.sock = -1;

    return
}

func (self *Client) SetServer(host string, port int) *Client {
    self.host = host
    self.port = port
    return self
}

func (client *Client) SetLimits(offset int, limit int, max int, cutoff int) *Client {
    client.offset = offset
    client.limit = limit
    if max > 0 {
        client.max_matches = max
    }
    if cutoff > 0 {
        client.cutoff = cutoff
    }
    return client
}


func (self *Client) Status() ([][]string, os.Error) {
    body := make([]byte, 4)
    binary.BigEndian.PutUint32(body[0:4], 1)
    
    rest, err := self.simpleQuery(searchdCommandStatus, verCommandStatus, len(body), body)
    if err != nil {
        return nil, err
    }
    
    rows := binary.BigEndian.Uint32(rest[0:4])
    cols := binary.BigEndian.Uint32(rest[4:8])
    rest = rest[8:]
    
    response := make([][]string, rows)
    for i := 0; i < int(rows); i++ {
        response[i] = make([]string, cols)
        for j := 0; j < int(cols); j++ {
            len := binary.BigEndian.Uint32(rest[0:4])
            response[i][j] = bytes.NewBuffer(rest[4:4+len]).String()
            rest = rest[4+len:]
        }
    }
    
    return response, nil
}

func (self *Client) connect() (*net.TCPConn, os.Error) {
    addr, err := net.ResolveTCPAddr(fmt.Sprintf("%s:%d", self.host, self.port))
    if err != nil {
        return nil, err
    }

    conn, err := net.DialTCP("tcp", nil, addr)
    if err != nil {
        return nil, err
    }

    ver_body := make([]byte, 4)
    binary.BigEndian.PutUint16(ver_body[0:4], 1)
    _, err = conn.Write(ver_body)
    if err != nil {
        return nil, err
    }
    ver_bits, err := ioutil.ReadAll(io.LimitReader(conn, 4))
    if err != nil {
        return nil, err
    }
    my_proto := binary.BigEndian.Uint32(ver_bits[0:4])
    
    if my_proto < 1 {
        self.error = fmt.Sprintf("expected searchd protocol version 1+, got version %d", my_proto)
        return nil, SphinxError{os.ErrorString(self.error)}
    }
    return conn, nil
}

func (self *Client) simpleQuery(command int, version int, size int, body []byte) ([]byte, os.Error) {
    conn, err := self.connect()
    if err != nil {
        return nil, err
    }

    header := make([]byte, 8 + len(body))
    binary.BigEndian.PutUint16(header[0:2], uint16(command))
    binary.BigEndian.PutUint16(header[2:4], uint16(version))
    binary.BigEndian.PutUint32(header[4:8], uint32(size))

	request := bytes.Add(header, body);

    _, err = conn.Write(request)
    if err != nil {
        return nil, err
    }
    
    return self.getResponse(conn)
}

func (self *Client) getResponse(conn *net.TCPConn) ([]byte, os.Error) {
    size_bits, _ := ioutil.ReadAll(io.LimitReader(conn, 8))
    status := binary.BigEndian.Uint16(size_bits[0:2])
    // ver := binary.BigEndian.Uint16(size_bits[2:4])
    size := binary.BigEndian.Uint32(size_bits[4:8])
    rest, _ := ioutil.ReadAll(io.LimitReader(conn, int64(size)))
    
    switch status {
    case SearchdOk:
    case SearchdWarning:
        wlen := binary.BigEndian.Uint32(rest[0:4])
        self.warning = bytes.NewBuffer(rest[4:wlen]).String()
        rest = rest[4+wlen:]
    case SearchdError, SearchdRetry:
        wlen := binary.BigEndian.Uint32(rest[0:4])
        self.error = bytes.NewBuffer(rest[4:wlen]).String()
        return nil, SphinxError{os.ErrorString(self.error)}
    default:
        self.error = fmt.Sprintf("unknown status code (status=%d)", status)
        return nil, SphinxError{os.ErrorString(self.error)}
    }
    return rest, nil
}
