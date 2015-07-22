package cache

import (
	"errors"
	"fmt"
	"message"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
	"strconv"
	"strings"
	"time"
)

const (
	DELIMETER rune = '!'
)

type Cache struct {
	cfg              *config.Config
	ledis_connection *ledis.Ledis
	db               *ledis.DB
}

/* New(string,bool) initializes a cache object,
takes as input database name and a boolean parameter stating whether a compression is needed or not.
*/
func New(dataDir string) (*Cache, error) {
	cfg := config.NewConfigDefault()
	dataDir = fmt.Sprint(cfg.DataDir, "/", dataDir)
	cfg.DataDir = dataDir
	cfg.Replication.Compression = false
	ledis_connection, err := ledis.Open(cfg)
	if err != nil {
		panic("Resource temporary unavailable, error opening a Connection!!!")
	}
	db, err := ledis_connection.Select(0)
	return &Cache{
		cfg,
		ledis_connection,
		db,
	}, err
}

func makeTimestampKey(key string) string {
	return key + "!timestamp"
}

/* SetData(string,string,int64) stores a key-value pair in the database,
takes as input key, value and key-expiry time.
*/
func (c *Cache) SetData(m *message.Message, time_to_live int64) (err error, msg *message.Message) {

	key := m.Key
	value := m.Value
	timestamp := m.Timestamp
	ttl := m.Ttl
	action := m.Action

	if len(value) == 0 {
		err = errors.New("value is empty")
		return
	}

	if key == "" {
		err = errors.New("key is empty")
	}

	if timestamp == 0 {
		err = errors.New("timestamp not set")
	}

	if strings.ContainsRune(key, DELIMETER) {
		err = errors.New("Key Error: Invalid key")
		return
	}

	keyb := []byte(key)
	//if the cache has fresher value then dont update it
	if val, err1 := c.db.Get(keyb); err1 == nil && len(val) != 0 {

		timestamp_key := []byte(makeTimestampKey(key))
		timestampb, _ := c.db.Get(timestamp_key)

		if len(timestampb) == 0 {
			timestampb = []byte("0")
		}

		local_timestamp, err3 := strconv.ParseInt(string(timestampb), 0, 64)

		if err3 == nil && local_timestamp > timestamp {
			err = errors.New("STALEDATA")
			msg = message.SetMessage(action, key, val, local_timestamp, ttl)
			return
		}

	}

	currentTimestamp := time.Now().Unix()
	time_to_live = time_to_live - (currentTimestamp - timestamp)
	err = c.db.SetEX([]byte(key), time_to_live, []byte(value))
	if err == nil {
		timestampS := strconv.FormatInt(timestamp, 10)
		err = c.db.SetEX([]byte(makeTimestampKey(key)), time_to_live, []byte(timestampS))
	}

	return
}

/* GetData(string)  takes a key as input a and returns the corresponding value stored in the database
 */
func (c *Cache) GetData(key string) (value string, err error) {

	val, err := c.db.Get([]byte(key))

	if err != nil {
		return
	}
	value = string(val)
	return
}

/* DeleteData(string)  deletes a particular key from the database
 */
func (c *Cache) DeleteData(key string) (err error) {
	_, err = c.db.Del([]byte(key))
	if err == nil {
		timestamp_key := []byte(makeTimestampKey(key))
		_, err = c.db.Del([]byte(timestamp_key))
	}
	return
}

/* UpdateTTL(string, int64)  updates expiry time of the given key.
 */
func (c *Cache) UpdateTTL(key string, duration int64) (set_time int64, err error) {
	set_time, err = c.db.Expire([]byte(key), duration)
	return
}

/* CloseConnection() closes the connection with the database.
 */
func (c *Cache) CloseConnection() {
	c.ledis_connection.Close()
}

func (c *Cache) GetAll() (values []string,err error) {
	cursor := []byte("")
	allKeys := [][]byte{}
	for { 
        	keys, err := c.db.Scan(ledis.DataType(1),cursor,1000,true,"")
		if err != nil {
			allKeys = append(allKeys,keys...)
        	}
		if len(keys) == 0 {
			break
		}
		cursor = keys[len(keys)-1]
	}
        values = make([]string,len(allKeys))
	totalKeys := len(allKeys) 
	for i :=0 ; i< totalKeys; i++ {
		/* Use MGet to get values of multiple keys */
		value, err := c.db.Get(allKeys[i])
		if err != nil {
                   values = append(values, string(value))
                }
        }
        return values,err
}
	
