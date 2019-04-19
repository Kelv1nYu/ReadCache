package com.kelv1n;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.HashSet;
import java.util.Set;

public final class MemcachedCache implements KVCache{

    private class ObjectWithCas {

        Object object;
        long cas;

        ObjectWithCas(Object object, long cas) {
            this.setObject(object);
            this.setCas(cas);
        }

        public Object getObject() {
            return object;
        }

        public void setObject(Object object) {
            this.object = object;
        }

        public long getCas() {
            return cas;
        }

        public void setCas(long cas) {
            this.cas = cas;
        }
    }

    private final MemcachedConfig config;
    private final MemcachedClient client;



    public MemcachedCache(){

        config = null;
        try{
            client = new MemcachedClient(config.getConnectionFactory(), config.getAddresses());
        }catch (IOException e){
            throw new RuntimeException();
        }
    }

    private ObjectWithCas getGroup(String groupKey) {

        ObjectWithCas groups = null;
        CASValue<Object> retrieved = null;

        if (config.isUsingAsynGet()) {
            Future<CASValue<Object>> future;
            if (config.isCompressionEnabled()) {
                //future = client.asyncGets(groupKey, new CompressorTranscoder());
                future = client.asyncGets(groupKey);
            } else {
                future = client.asyncGets(groupKey);
            }

            try {
                retrieved = future.get(config.getTimeOut(), config.getTimeUnit());
            } catch (Exception e) {
                future.cancel(false);
            }
        } else {
            if (config.isCompressionEnabled()) {
                //retrieved = client.gets(groupKey, new CompressorTranscoder());
            } else {
                retrieved = client.gets(groupKey);
            }
        }

        if (retrieved == null) {
            return null;
        }


        if (groups == null) {
            return null;
        }

        return groups;
    }

    public void removeGroup(Object key) {

        boolean jobDone = false;

        while (!jobDone) {
            ObjectWithCas group = getGroup(key.toString());
            Set<String> groupValues;

            if (group == null || group.getObject() == null) {
                return;
            }

            groupValues = (Set<String>) group.getObject();

            for (String eventId : groupValues) {
                client.delete(eventId);
            }

            groupValues = (Set<String>) group.getObject();
            groupValues.clear();

            jobDone = storeInMemcached(key.toString(), group);
        }
    }

    private boolean tryToAdd(String keyString, Object value) {
        if (value != null && !Serializable.class.isAssignableFrom(value.getClass())) {
            return false;
        }

        boolean done;
        OperationFuture<Boolean> result;

        if (config.isCompressionEnabled()) {
            result = client.add(keyString, config.getExpiration(), value);
        } else {
            result = client.add(keyString, config.getExpiration(), value);
        }

        try {
            done = result.get();
        } catch (InterruptedException e) {
            done = false;
        } catch (Exception e) {
            done = false;
        }

        return done;
    }

    @Override
    public boolean isExists(Object key){
        Object k = client.get(key.toString());
        if(k != null){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public Object put(Object key, Object value){
        client.set(key.toString(), config.getExpiration(), value);
        boolean jobDone = false;

        while (!jobDone) {
            ObjectWithCas group = getGroup("VectorId");
            Set<String> groupValues;

            if (group == null || group.getObject() == null) {
                groupValues = new HashSet<String>();
                groupValues.add(key.toString());

                jobDone = tryToAdd("VectorId", groupValues);
            } else {
                groupValues = (Set<String>) group.getObject();
                groupValues.add(key.toString());

                jobDone = storeInMemcached("VectorId", group);
            }
        }
        return null;
    }

    private boolean storeInMemcached(String key, ObjectWithCas value){

        CASResponse response;

        if (config.isCompressionEnabled()) {
            //response = client.cas(key, value.getCas(), value.getObject(), new CompressorTranscoder());
            response = client.cas(key, value.getCas(), value.getObject());
        } else {
            response = client.cas(key, value.getCas(), value.getObject());
        }

        return (response.equals(CASResponse.OBSERVE_MODIFIED) || response.equals(CASResponse.OK));
    }

    @Override
    public Object get(Object key){

        return client.get(key.toString());
    }

    @Override
    public Object remove(Object key){
        client.delete(key.toString());
        return null;
    }

    @Override
    public void clear(){
        //List<String> data = new ArrayList<>();
        removeGroup("VectorId");
    }

    @Override
    public int getSize(){
        return Integer.MAX_VALUE;
    }

    @Override
    public void finalize() throws Throwable{
        client.shutdown(config.getTimeOut(), config.getTimeUnit());
        super.finalize();
    }
}
