package com.kelv1n;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class IntVectorReader extends ArrowReader<Integer>{

    private String fieldName;
    private IntVector currentVector;


    IntVectorReader(VectorSchemaRoot schemaRoot,
                    String fieldName,
                    BufferAllocator bufferAllocator){
        super(bufferAllocator, schemaRoot);
        this.fieldName = fieldName;
    }

    @Override
    public void read(ByteBuffer readableData, CompletableFuture<ValueVector> future){
        loadData2Vector(readableData);
        currentVector = (IntVector) schemaRoot.getVector(fieldName);
        future.complete(currentVector);
        /*int a = currentVector.getValueCount();
        System.out.println("currentCountOfEvents of IntVector" + a);

        for(int i = 0; i < a; i++){
            System.out.println(currentVector.get(i));
        }*/
        //return currentVector;
    }
}

