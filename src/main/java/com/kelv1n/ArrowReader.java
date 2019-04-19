package com.kelv1n;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public abstract class ArrowReader<T> {

    final VectorSchemaRoot schemaRoot;
    final BufferAllocator allocator;
    final VectorLoader vectorLoader;

    ArrowReader(BufferAllocator bufferAllocator,
                VectorSchemaRoot schemaRoot){
        this.allocator = bufferAllocator;
        this.schemaRoot = schemaRoot;
        vectorLoader = new VectorLoader(schemaRoot);

    }
    abstract void read(ByteBuffer readableData, CompletableFuture<ValueVector> future);

    protected void loadData2Vector(ByteBuffer readableData){
        ArrowRecordBatch recordBatch = null;
        // deserialize vector data
        try {
            recordBatch = ArrowSerializer.deserializeRecordBatch(readableData, allocator);
            vectorLoader.load(recordBatch);
        } catch (IOException ioe) {
            //log.error("Deserialize from entry to ArrowRecordBatch fail due to: ", ioe);
            //throw ioe;
        } finally {
            if (null != recordBatch) {
                recordBatch.close();
            }
        }
    }
}
