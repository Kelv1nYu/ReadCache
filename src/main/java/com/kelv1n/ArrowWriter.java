package com.kelv1n;

import lombok.Getter;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ArrowWriter<V> {
    final ValueVector valueVector;
    private final VectorSchemaRoot schemaRoot;
    int currentCountOfEvents = 0;
    private long currentSeqNum = -1;
    List<Long> seqNumsInVector = new ArrayList<>();
    ArrowCache<Long, ByteBuffer> arrowCache;

    @Getter
    private final ConcurrentHashMap<Long, Types.MinorType> seqNum2VectorName = new ConcurrentHashMap<>();
    private VectorSchemaRoot schemaRoot1;

    public ArrowWriter(VectorSchemaRoot schemaRoot,
                       ValueVector valueVector,
                       ArrowCache arrowCache){

        this.schemaRoot = schemaRoot;
        this.valueVector = valueVector;
        this.arrowCache = arrowCache;

    }

    abstract void writeVector(V value);

    public void writeArrow(V value, long seqNum){
        //long freezedSeqNum;

        synchronized (valueVector){
            if (seqNum != currentSeqNum){
                long freezedCurrentSeqNum = currentSeqNum;
                currentSeqNum = seqNum;

                if(currentCountOfEvents != 0) {
                    int freezedCountOfEvents = currentCountOfEvents;
                    storeInCache(freezedCountOfEvents, freezedCurrentSeqNum);

                }
            }
            writeVector(value);
        }

    }

    public void storeInCache(int freezedCountOfEvents, long seqNum){
        valueVector.setValueCount(freezedCountOfEvents);
        schemaRoot.setRowCount(freezedCountOfEvents);
        //System.out.println(schemaRoot.getFieldVectors().get(0).getMinorType());
        //System.out.println(valueVector.getField());
        //System.out.println(valueVector.getField().getName());
        //System.out.println(valueVector.getField().getFieldType());
        //System.out.println(valueVector.getField().getType());
        //System.out.println(valueVector.getMinorType());
        Types.MinorType mt = valueVector.getMinorType();
        ArrowRecordBatch recordBatch = new VectorUnloader(schemaRoot).getRecordBatch();
        ByteBuffer serializedData = ArrowSerializer.serialize(recordBatch);
        serializedData.flip();
        ByteBuffer vectorName = ArrowSerializer.serializeMinorType(mt);
        int size = vectorName.capacity();
        ByteBuffer Data = ByteBuffer.allocate(4 + size + serializedData.remaining()).putInt(size).put(vectorName).put(serializedData);
        Data.flip();
        recordBatch.close();
        currentCountOfEvents = 0;
        seqNum2VectorName.put(seqNum, mt);
        arrowCache.put(seqNum, Data);

    }

    public int getCurrentCountOfEvents(){
        return currentCountOfEvents;
    }


    public void writeToBK(){

    }
}
