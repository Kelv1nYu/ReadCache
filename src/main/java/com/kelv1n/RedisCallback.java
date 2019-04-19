package com.kelv1n;

import redis.clients.jedis.Jedis;

public interface RedisCallback {

    /**
     *
     * @param jedis
     * @return
     */
    Object connectRedis(Jedis jedis);
}
