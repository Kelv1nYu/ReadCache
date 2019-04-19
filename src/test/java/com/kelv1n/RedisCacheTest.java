package com.kelv1n;


import net.spy.memcached.MemcachedClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class RedisCacheTest {

    private static final String DEFAULT_ID = "REDIS";
    private static final String EMPTY_SCHEMA_PATH = "";
    private BufferAllocator allocator;
    private byte[] serializedData;

    private static RedisCache cache;


    Jedis gJedis = null;

    public RedisCacheTest(){

    }

    @BeforeClass
    public static void createRedisCache(){
        cache = new RedisCache();
    }

    @Before
    public void init(){
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void terminate() throws Exception{
        allocator.close();
    }

    @Test
    public void testIntVector(){

        try(final IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator)){
            boolean error = false;
            int initialCapacity = 16;

            intVector.setInitialCapacity(initialCapacity);
            assertEquals(0, intVector.getValueCapacity());

            intVector.allocateNew();
            assertEquals(initialCapacity, intVector.getValueCapacity());

            int j = 1;
            for(int i = 0; i < 16; i += 2){
                intVector.set(i,j);
                j++;
            }



            j = 1;
            for (int i = 0; i < 16; i += 2) {
                assertEquals("unexpected value at index: " + i, j, intVector.get(i));
                cache.put("vector", intVector.get(i));
                j++;
            }

            List<byte[]> list = (List<byte[]>) cache.get("vector");

            List<String> data = new ArrayList<>();
            for(int i = 0; i < list.size(); i++){
                String str = new String(list.get(i));
                data.add(str);
            }

            System.out.println(data);
        }

        cache.clear();
    }

}