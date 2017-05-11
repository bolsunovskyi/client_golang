package prometheus

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"context"
	"strconv"
	"fmt"
	"errors"
)

var storage KeyValue = &localKeyValue{
	storage:	map[string]float64{},
}

//KeyValue key value storage interface
type KeyValue interface {
	Get(key string) (float64, error)
	Set(key string, value float64) error
	Remove(key string) error
}

type etcdKeyValue struct {
	KeyAPI *etcd.KeysAPI
	prefix string
}

type localKeyValue struct {
	storage map[string]float64
}

func InitETCDStorage(prefix string, hosts []string) {
	cfg := etcd.Config{
		Endpoints:               hosts,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: 5 * time.Second,
	}
	c, _ := etcd.New(cfg)
	kapi := etcd.NewKeysAPI(c)

	storage = etcdKeyValue{
		KeyAPI: &kapi,
		prefix:	prefix,
	}
}

func (kv *localKeyValue) Get(key string) (float64, error) {
	if v, ok := kv.storage[key]; ok {
		return v, nil
	}

	return 0, errors.New("Value not found")
}

func (kv *localKeyValue) Set(key string, value float64) error {
	kv.storage[key] = value
	return nil
}

func (kv *localKeyValue) Remove(key string) error {
	delete(kv.storage, key)
	return nil
}

func (kv etcdKeyValue) Get(key string) (float64, error) {
	rsp, err := (*kv.KeyAPI).Get(context.Background(), fmt.Sprintf("%s/%s", kv.prefix, key), nil)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(rsp.Node.Value, 64)
}

func (kv etcdKeyValue) Set(key string, value float64) error {
	_, err := (*kv.KeyAPI).Set(
		context.Background(),
		fmt.Sprintf("%s/%s", kv.prefix, key),
		fmt.Sprintf("%.0f", value), &etcd.SetOptions{})
	return err
}

func (kv etcdKeyValue) Remove(key string) error {
	_, err := (*kv.KeyAPI).Delete(context.Background(), fmt.Sprintf("%s/%s", kv.prefix, key), nil)
	return err
}
