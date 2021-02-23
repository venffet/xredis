**xredis: a redis-balancer wrapper**

Xredis: a redis-balancer wrapper which wrap a redis-balancer as a Client"gopkg.in/redis.v5/redis.go:Client" with auto failover;
compatible with lagacy code wrote by Client in "redis.v5".

It is especially siutable for twemproxy cluster which connect the same group of redis-server behind. 
