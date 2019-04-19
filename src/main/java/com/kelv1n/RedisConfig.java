package com.kelv1n;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import sun.plugin2.message.Serializer;

public class RedisConfig extends JedisPoolConfig {
    private String host = Protocol.DEFAULT_HOST;
    private int port = Protocol.DEFAULT_PORT;
    private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
    private String password;
    private boolean ssl;

    public boolean isSsl(){
        return ssl;
    }

    public void setSsl(boolean ssl){
        this.ssl = ssl;
    }

    public String getHost(){
        return host;
    }

    public void setHost(String host){
        if( null == host || "".equals(host) ){
            host = Protocol.DEFAULT_HOST;
        }
        this.host = host;
    }

    public int getPort(){
        return port;
    }

    public void setPort(int port){
        this.port = port;
    }

    public String getPassword(){
        return password;
    }

    public void setPassword(String password){
        if("".equals(password)){
            password = null;
        }
        this.password = password;
    }

    public int getConnectionTimeout(){
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout){
        this.connectionTimeout = connectionTimeout;
    }

   /* public Serializer getSeralizer(){
        return serializer;
    }

    public void setSerializer(Serializer serializer){
        this.serializer = serializer;
    }*/


}
