package goredis

import (
	"time"
	"errors"
	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

type ClusterClient struct {
	masters 	*redisc.Cluster
	masterName 	string
}

func NewClusterClient(masterName string, addrs []string, option *Option) *ClusterClient {
	cli := &ClusterClient{}
	cli.Init(masterName, addrs, option)
	return cli
}

func (this *ClusterClient) Init(masterName string, addrs []string, option *Option)  {
	cluster := &redisc.Cluster{
		StartupNodes: addrs,
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(5 * time.Second)},
		CreatePool:   func(address string, options ...redis.DialOption) (*redis.Pool, error){
			pool := &redis.Pool{
				MaxIdle:     option.PoolMaxIdle,
				MaxActive:   option.PoolMaxActive,
				IdleTimeout: option.PoolIdleTimeout,
				Dial: func() (redis.Conn, error) {
					c, err := redis.Dial("tcp", address, options...)
					if err != nil {
						return nil, err
					}
					if option.Password != "" {
						if _, err := c.Do("AUTH", option.Password); err != nil {
							c.Close()
							return nil, err
						}
					}
					if _, err := c.Do("SELECT", option.DBIndex); err != nil {
						c.Close()
						return nil, err
					}
					return c, err
				},
				TestOnBorrow: func(c redis.Conn, t time.Time) error {
					_, err := c.Do("PING")
					return err
				},
			}
			return pool, nil
		},
	}
	cluster.Refresh()
	this.masters = cluster
}

func (this *ClusterClient) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	conn := this.masters.Get()
	if conn != nil {
		defer conn.Close()
		return conn.Do(commandName, args...)
	} else {
		return nil, errors.New("[redis] Can't get redis conn!")
	}
}

func (this *ClusterClient) GetConn() (reply interface{}, err error) {
	conn := this.masters.Get()
	if conn != nil {
		return this.masters.Get(), nil
	} else {
		return nil, errors.New("[redis] Can't get redis conn!")
	}
}