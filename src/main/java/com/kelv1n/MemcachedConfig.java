package com.kelv1n;

import static java.lang.String.format;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ConnectionFactory;

public class MemcachedConfig {
    private String keyPrefix;

    private ConnectionFactory connectionFactory;

    private List<InetSocketAddress> addresses;

    private boolean usingAsynGet;

    private boolean compressionEnabled;

    private int expiration;

    private int timeOut;

    private TimeUnit timeUnit;

    public String getKeyPrefix(){
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix){
        this.keyPrefix = keyPrefix;
    }

    public ConnectionFactory getConnectionFactory(){
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory){
        this.connectionFactory = connectionFactory;
    }

    public List<InetSocketAddress> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<InetSocketAddress> addresses){
        this.addresses = addresses;
    }

    public boolean isUsingAsynGet(){
        return usingAsynGet;
    }

    public void setUsingAsynGet(){
        this.usingAsynGet = usingAsynGet;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    public void setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut){
        this.timeOut = timeOut;
    }

    public TimeUnit getTimeUnit( ){
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit){
        this.timeUnit = timeUnit;
    }

}
