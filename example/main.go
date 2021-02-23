package main

import (
	"fmt"

	"github.com/venffet/xredis"
)

func main() {
	client := xredis.NewClusterClient(&xredis.ClusterOptions{
		Addrs:    []string{"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"},
		Password: "",
	})

	for i := 0; i < 20; i++ {

		str, err := client.Ping().Result()
		fmt.Println(str, err)

	}

	pipe := client.Pipeline()

	pipe.Set("KKK", 111, 0)

	pipe.Set("YYY", 222, 0)

	cmds, err := pipe.Exec()

	fmt.Println(cmds, err)

	client.Close()
}
