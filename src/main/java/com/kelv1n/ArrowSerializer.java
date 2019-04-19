package com.kelv1n;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.flatbuffers.FlatBufferBuilder;
import io.netty.buffer.ArrowBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types;


public class ArrowSerializer {

    public static ByteBuffer serialize(ArrowRecordBatch batch){
        int bodyLength = batch.computeBodyLength();
        checkArgument(bodyLength % 8 == 0, "ArrowRecordBatch body should be aligned at 8-byte boundaries");

        FlatBufferBuilder builder = new FlatBufferBuilder();
        int batchOffset = batch.writeTo(builder);

        ByteBuffer serializedMessage = MessageSerializer.serializeMessage(
                builder, MessageHeader.RecordBatch, batchOffset, bodyLength);

        int metadataLength = serializedMessage.remaining();

        ByteBuffer buffers = getBatchBuffers(batch);
        buffers.flip();

        // metadataLength[Int] + len(serializedMessage) + len(buffers)
        ByteBuffer message = ByteBuffer.allocate(4 + metadataLength + buffers.remaining());
        //ByteBuffer message = ByteBuffer.allocate(buffers.remaining());

        byte[] metaLen = Bytes.toBytes(metadataLength);
        message.put(metaLen);
        message.put(serializedMessage);
        message.put(buffers);
        return message;
    }

    public static ByteBuffer getBatchBuffers(ArrowRecordBatch batch) {
        List<ArrowBuf> buffers = batch.getBuffers();
        List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
        int len = 0;
        for (int i = 0; i < buffers.size(); i++) {
            ArrowBuffer layout = buffersLayout.get(i);
            len += layout.getOffset();
            len += layout.getSize();
        }

        ByteBuffer message = ByteBuffer.allocate(len);

        for (int i = 0; i < buffers.size(); i++) {
            ArrowBuf buffer = buffers.get(i);
            ArrowBuffer layout = buffersLayout.get(i);
            long startPosition = layout.getOffset();
            // (len - message.remaining()) represent current writer index of bytebuffer
            if (startPosition != (len - message.remaining())) {
                message.put(new byte[(int) (startPosition - (len - message.remaining()))]);
            }
            message.put(buffer.nioBuffer(buffer.readerIndex(), buffer.readableBytes()));
            if ((len - message.remaining()) != startPosition + layout.getSize()) {
                throw new IllegalStateException("wrong buffer size: " + (len - message.remaining())
                        + " != " + startPosition + layout.getSize());
            }
        }
        return message;
    }

    public static ArrowRecordBatch deserializeRecordBatch(ByteBuffer data,
                                                          BufferAllocator alloc) throws IOException {
        int totalLen = data.remaining();
        ArrowBuf buffer = alloc.buffer(totalLen);
        buffer.nioBuffer(0, totalLen).put(data);
        byte[] metaBytes = new byte[4];
        buffer.getBytes(0, metaBytes, 0, 4);
        int metadataLength = Bytes.toInt(metaBytes, 0);
        ArrowBuf metadataBuffer = buffer.slice(4, metadataLength);
        Message messageFB =
                Message.getRootAsMessage(metadataBuffer.nioBuffer().asReadOnlyBuffer());
        RecordBatch recordBatchFB = (RecordBatch) messageFB.header(new RecordBatch());

        // Now read the body
        final ArrowBuf body = buffer.slice(metadataLength + 4, totalLen - 4 - metadataLength);
        return MessageSerializer.deserializeRecordBatch(recordBatchFB, body);
    }

    public static ByteBuffer serializeMinorType(Types.MinorType mt){
        String vectorName = mt.name();
        return ByteBuffer.wrap(vectorName.getBytes());
    }

    public static String deserializerMinorType(ByteBuffer data){
        Charset charset = Charset.forName("utf-8");

        return charset.decode(data).toString();

    }

}
