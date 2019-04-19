package com.kelv1n;

import org.apache.arrow.memory.BufferAllocator;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class Float4VectorReader extends ArrowReader<Float> {
    private String fieldName;
    private Float4Vector currentVector;


    Float4VectorReader(VectorSchemaRoot schemaRoot,
                    String fieldName,
                    BufferAllocator bufferAllocator){
        super(bufferAllocator, schemaRoot);
        this.fieldName = fieldName;
    }

    @Override
    public void read(ByteBuffer readableData, CompletableFuture<ValueVector> future){
        loadData2Vector(readableData);
        currentVector = (Float4Vector) schemaRoot.getVector(fieldName);
        future.complete(currentVector);
        /*int a = currentVector.getValueCount();
        System.out.println(a);

        for(int i = 0; i < a; i++){
            System.out.println(currentVector.get(i));
        }*/
    }
}
