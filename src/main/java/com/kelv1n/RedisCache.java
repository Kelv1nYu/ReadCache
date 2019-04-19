package com.kelv1n;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public final class RedisCache implements KVCache{

    private boolean isConnected = false;

    private  String id;

    private static JedisPool pool;

    private final RedisConfig redisConfig;

    private static final Charset utf8Charset = Charset.forName("UTF-8");

    public RedisCache(){

        redisConfig = new RedisConfig();
        pool = new JedisPool(redisConfig, redisConfig.getHost(), redisConfig.getPort(), redisConfig.getConnectionTimeout(),
                redisConfig.getPassword(), redisConfig.isSsl());
    }

    private Object connect(RedisCallback callback) {
        Jedis jedis = pool.getResource();
        try {
            return callback.connectRedis(jedis);
        } finally {
            jedis.close();
        }
    }

    @Override
    public boolean isExists(final Object key){
        return (boolean)connect(new RedisCallback() {
            @Override
            public Object connectRedis(Jedis jedis) {
                return jedis.exists(key.toString().getBytes());
            }
        });
    }


    public boolean isExists(Jedis jedis, final Object key){
        return jedis.exists(key.toString());
    }

    @Override
    public Object put(final Object key, final Object value){

        return connect(new RedisCallback() {
            @Override
            public Object connectRedis(Jedis jedis) {
                jedis.rpush(key.toString().getBytes(utf8Charset), value.toString().getBytes(utf8Charset));
                return null;
            }
        });

    }

    public void putData(Jedis jedis, final Object key, final Object value){
        jedis.rpush(key.toString().getBytes(), value.toString().getBytes());
    }

    @Override
    public Object get(final Object key){
        return connect(new RedisCallback() {
            @Override
            public Object connectRedis(Jedis jedis) {
                /*List<byte[]> list = jedis.lrange(key.toString().getBytes(utf8Charset), 0, -1);
                List<String> data = new ArrayList<>();
                for(int i = 0; i < list.size(); i++){
                    String str = new String(list.get(i));
                    data.add(str);
                }
                return data;*/
                //V v = jedis.lrange(key.toString().getBytes(utf8Charset), 0, -1);
                return jedis.lrange(key.toString().getBytes(utf8Charset), 0, -1);
            }
        });

    }

    public Object getData(Jedis jedis, final Object key){

        List<byte[]> list = jedis.lrange(key.toString().getBytes(utf8Charset), 0, -1);
        List<String> data = new ArrayList<>();
        for(int i = 0; i < list.size(); i++){
            String str = new String(list.get(i));
            data.add(str);
        }
        return data;
    }



    @Override
    public Object remove(final Object key){
        return connect(new RedisCallback() {
            @Override
            public Object connectRedis(Jedis jedis) {
                jedis.del(key.toString().getBytes());
                return null;
            }
        });
    }

    public Object removeData(Jedis jedis, final Object key){
        return jedis.del(key.toString().getBytes());
    }

    @Override
    public void clear(){
        connect(new RedisCallback() {
            @Override
            public Object connectRedis(Jedis jedis) {
                jedis.flushAll();
                return null;
            }
        });
    }

    public void clear(Jedis jedis){
        jedis.flushAll();
    }

    @Override
    public int getSize(){
        return (Integer)connect(new RedisCallback() {
            @Override
            public Object connectRedis(Jedis jedis) {
                return null;
            }
        });
    }
}
