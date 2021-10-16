/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.rpc.net;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.XLog;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.perf.TcpPerfItem;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessageV3;
import com.mysql.cj.polarx.protobuf.PolarxConnection;
import com.mysql.cj.polarx.protobuf.PolarxNotice;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.mysql.cj.polarx.protobuf.PolarxSession;
import com.mysql.cj.polarx.protobuf.PolarxSql;
import com.mysql.cj.x.protobuf.Polarx;
import com.mysql.cj.x.protobuf.PolarxExecPlan;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @version 1.0
 */
public class NIOClient implements NIOConnection {

    private static final int socketRecvBuffer = 64 * 1024;
    private static final int socketSendBuffer = 32 * 1024;

    private volatile SocketChannel channel = null; // Valid after connected.
    private NIOProcessor processor = null; // Valid after registered.

    private SelectionKey processKey = null; // Valid after registered.
    private final ReentrantLock keyLock = new ReentrantLock();

    private ByteBuffer readBuffer = null; // When with received data. Init once and set to null after cleanup.
    private ByteBuffer bigReadBuffer = null; // For big packet.
    private final ReentrantLock readLock = new ReentrantLock(); // This lock to prevent conflict between cleanup & read.

    private final AtomicBoolean bigBufferMark = new AtomicBoolean(false);
    private final AtomicBoolean bigBufferClean = new AtomicBoolean(false);

    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private ByteBuffer lastWrite = null; // Last buffer of queue for tail append and buffer reuse.
    private final ReentrantLock writeLock = new ReentrantLock();

    private volatile boolean isRegistered = false;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final Consumer<Collection<XPacket>> consumer; // Protocol consumer.
    private final Consumer<Throwable> fatalCallback; // Error callback.
    private final XClient client; // For perf data collection.

    /**
     * Usage:
     * client.connect(new InetSocketAddress(SERVER_IP, SERVER_PORT), 10000);
     * client.setProcessor(PROCESSOR);
     * PROCESSOR.postRegister(client);
     *
     * @param consumer Consumer.
     */
    public NIOClient(Consumer<Collection<XPacket>> consumer, Consumer<Throwable> fatalCallback,
                     XClient client) {
        this.consumer = consumer;
        this.fatalCallback = fatalCallback;
        this.client = client;
    }

    public void setProcessor(NIOProcessor processor) {
        this.processor = processor;
    }

    public void fillTcpPhysicalInfo(TcpPerfItem item) {
        item.setSocketSendBufferSize(socketSendBuffer);
        item.setSocketRecvBufferSize(socketRecvBuffer);

        readLock.lock();
        try {
            item.setReadDirectBuffers(null == readBuffer ? 0 : 1);
            item.setReadHeapBuffers(null == bigReadBuffer ? 0 : 1);
        } finally {
            readLock.unlock();
        }

        writeLock.lock();
        try {
            long directCount = 0;
            long heapCount = 0;
            final Iterator<ByteBuffer> iterator = writeQueue.iterator();
            while (iterator.hasNext()) {
                final ByteBuffer buf = iterator.next();
                if (buf != null) {
                    if (buf.isDirect()) {
                        ++directCount;
                    } else {
                        ++heapCount;
                    }
                }
            }
            item.setWriteDirectBuffers(directCount);
            item.setWriteHeapBuffers(heapCount);
        } finally {
            writeLock.unlock();
        }

        item.setReactorRegistered(isRegistered);
        item.setSocketClosed(isClosed.get());
    }

    public synchronized void connect(SocketAddress address, int timeout) throws IOException {
        if (isClosed.get()) {
            throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_CLIENT,
                this + " closed.");
        }
        if (this.channel != null) {
            throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_CLIENT,
                this + " already connected.");
        }

        final long startNanos = System.nanoTime();
        final SocketChannel c = SocketChannel.open();
        try {
            c.setOption(StandardSocketOptions.TCP_NODELAY, true);
            c.socket().connect(address, timeout);
            if (c.socket().getSendBufferSize() < socketSendBuffer) {
                c.socket().setSendBufferSize(socketSendBuffer);
            }
            if (c.socket().getReceiveBufferSize() < socketRecvBuffer) {
                c.socket().setReceiveBufferSize(socketRecvBuffer);
            }
            c.configureBlocking(false); // Switch to non-block mode.
            this.channel = c;
            final long endNanos = System.nanoTime();
            XLog.XLogLogger.info(this + " connect success, connect time: " + (endNanos - startNanos) / 1000L + "us.");
        } catch (Throwable e) {
            final long endNanos = System.nanoTime();
            XLog.XLogLogger.error(this + " connect error with timeout " + timeout + "ms. real:"
                + (endNanos - startNanos) / 1000L + "us.");
            XLog.XLogLogger.error(e);
            this.channel = null;
            // Close and free the resources.
            try {
                final Socket socket = c.socket();
                if (socket != null) {
                    socket.close();
                }
            } catch (Throwable ignore) {
            }
            try {
                c.close();
            } catch (Throwable ignore) {
            }
            throw e;
        }
    }

    public boolean isPending() {
        return null == channel; // Connecting?
    }

    public boolean isValid() {
        final SocketChannel c = channel;
        return c != null && c.isOpen() && isRegistered;
    }

    private void clearSelectionKey() {
        final Lock lock = this.keyLock;
        lock.lock();
        try {
            final SelectionKey key = this.processKey;
            if (key != null && key.isValid()) {
                key.attach(null);
                key.cancel();
                processor.getReactor().getPerfCollection().getSocketCount().getAndDecrement();
            }
        } catch (Throwable ignore) {
        } finally {
            lock.unlock();
        }
    }

    private boolean closeSocket() {
        XLog.XLogLogger.info(this + " close socket.");
        clearSelectionKey();
        final SocketChannel c = this.channel;
        boolean socketClosed = true;
        boolean channelClosed;
        if (c != null) {
            final Socket socket = channel.socket();
            if (socket != null) {
                try {
                    socket.close();
                } catch (Throwable e) {
                }
                socketClosed = socket.isClosed();
            }
            try {
                c.close();
            } catch (Throwable ignore) {
            }
            channelClosed = !c.isOpen();
        } else {
            channelClosed = true;
        }
        return socketClosed && channelClosed;
    }

    private void cleanup() {
        readLock.lock();
        try {
            if (readBuffer != null) {
                processor.getBufferPool().recycle(readBuffer);
                readBuffer = null;
            }
        } finally {
            readLock.unlock();
        }

        writeLock.lock();
        try {
            ByteBuffer top;
            while ((top = writeQueue.poll()) != null) {
                processor.getBufferPool().recycle(top);
            }
            lastWrite = null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public synchronized void close() {
        if (!isClosed.get()) {
            closeSocket(); // Ignore the result and free the resource anyway.
            if (isClosed.compareAndSet(false, true)) {
                cleanup();
            }
        }
    }

    @Override
    public synchronized void register(Selector selector) throws IOException {
        try {
            processKey = channel.register(selector, SelectionKey.OP_READ, this);
            processor.getReactor().getPerfCollection().getSocketCount().getAndIncrement();
            isRegistered = true;
        } finally {
            if (isClosed.get()) {
                clearSelectionKey();
            }
        }
    }

    public void shrinkBuffer() {
        if (!bigBufferMark.compareAndSet(true, false)) {
            bigBufferClean.set(true);
        }
    }

    @Override
    public void read() throws IOException {
        // This always in single thread.
        final List<XPacket> batch = new ArrayList<>(32);

        readLock.lock();
        try {
            // To prevent reallocate after close.
            if (isClosed.get()) {
                throw new TddlRuntimeException(
                    com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_CLIENT,
                    this + " closed.");
            }

            // Select one buffer for recv.
            if (null == readBuffer) {
                readBuffer = processor.getBufferPool().allocate();
            }
            final ByteBuffer recvBuffer = bigReadBuffer != null ? bigReadBuffer : readBuffer;

            // recv.
            final int got = channel.read(recvBuffer);
            if (got < 0) {
                throw new TddlRuntimeException(
                    com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_CLIENT,
                    this + " EOF.");
            }

            // Record perf data.
            if (client != null) {
                // Record on per TCP and DN gather.
                client.getPerfCollection().getRecvNetCount().getAndIncrement();
                client.getPerfCollection().getRecvSize().getAndAdd(got);
                client.getPool().getPerfCollection().getRecvNetCount().getAndIncrement();
                client.getPool().getPerfCollection().getRecvSize().getAndAdd(got);
            }

            // Decode.
            final long maxPacketSize = XConnectionManager.getInstance().getMaxPacketSize();

            recvBuffer.flip();
            while (recvBuffer.remaining() >= XPacket.HEADER_SIZE) {
                final int packetSize =
                    recvBuffer.getInt(recvBuffer.position() + XPacket.LENGTH_OFFSET) - XPacket.TYPE_SIZE;

                // Check bad header.
                if (packetSize < 0 || packetSize > maxPacketSize) {
                    throw new TddlRuntimeException(
                        com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_BAD_PACKET,
                        this + ". Received packet length (" + packetSize + ") "
                            + "exceeds the allowed maximum (" + maxPacketSize + ").");
                }

                if (XPacket.HEADER_SIZE + packetSize > readBuffer.capacity()) {
                    // Larger than normal buffer.
                    bigBufferMark.set(true);
                    bigBufferClean.set(false);
                }

                if (recvBuffer.remaining() >= XPacket.HEADER_SIZE + packetSize) {
                    final long sid = recvBuffer.getLong();
                    if (XConfig.GALAXY_X_PROTOCOL) {
                        final long version = recvBuffer.get();
                        if (version != XPacket.VERSION) {
                            throw new TddlRuntimeException(
                                com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_BAD_PACKET,
                                this + ". Received packet version (" + version + ") unexpected.");
                        }
                    }
                    recvBuffer.position(recvBuffer.position() + XPacket.LENGTH_SIZE);
                    final byte type = recvBuffer.get();
                    final int nextPos = recvBuffer.position() + packetSize;
                    final int oldLimit = recvBuffer.limit();
                    recvBuffer.limit(nextPos);

                    // Decode packet.
                    try {
                        // Decode.
                        final int RDS80_REBASE = 100 - 19;
                        switch (type) {
                        case Polarx.ServerMessages.Type.OK_VALUE:
                            batch.add(new XPacket(sid, type, Polarx.Ok.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.ERROR_VALUE:
                            batch.add(new XPacket(sid, type, Polarx.Error.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.CONN_CAPABILITIES_VALUE:
                            batch.add(new XPacket(
                                sid, type, PolarxConnection.Capabilities.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE:
                            batch.add(new XPacket(
                                sid, type, PolarxSession.AuthenticateContinue.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE:
                            batch.add(new XPacket(
                                sid, type, PolarxSession.AuthenticateOk.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.NOTICE_VALUE:
                            batch.add(new XPacket(sid, type, PolarxNotice.Frame.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE:
                            if (XConfig.GALAXY_X_PROTOCOL) {
                                batch.add(new XPacket(
                                    sid, type, PolarxResultset.ColumnMetaDataCompatible.parseFrom(recvBuffer),
                                    packetSize));
                            } else {
                                batch.add(new XPacket(
                                    sid, type, PolarxResultset.ColumnMetaData.parseFrom(recvBuffer), packetSize));
                            }
                            break;

                        case Polarx.ServerMessages.Type.RESULTSET_ROW_VALUE:
                            batch.add(new XPacket(sid, type, PolarxResultset.Row.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE:
                            batch.add(new XPacket(
                                sid, type, PolarxResultset.FetchDone.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS_VALUE:
                            batch.add(new XPacket(sid, type,
                                PolarxResultset.FetchDoneMoreResultsets.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE:
                            batch.add(new XPacket(
                                sid, type, PolarxSql.StmtExecuteOk.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.RESULTSET_TOKEN_DONE_VALUE:
                        case RDS80_REBASE + Polarx.ServerMessages.Type.RESULTSET_TOKEN_DONE_VALUE:
                            batch.add(new XPacket(
                                sid, Polarx.ServerMessages.Type.RESULTSET_TOKEN_DONE_VALUE,
                                PolarxResultset.TokenDone.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.RESULTSET_TSO_VALUE:
                        case RDS80_REBASE + Polarx.ServerMessages.Type.RESULTSET_TSO_VALUE:
                            batch.add(new XPacket(
                                sid, Polarx.ServerMessages.Type.RESULTSET_TSO_VALUE,
                                PolarxExecPlan.ResultTSO.parseFrom(recvBuffer), packetSize));
                            break;

                        case Polarx.ServerMessages.Type.RESULTSET_CHUNK_VALUE:
                        case RDS80_REBASE + Polarx.ServerMessages.Type.RESULTSET_CHUNK_VALUE:
                            batch.add(new XPacket(
                                sid, Polarx.ServerMessages.Type.RESULTSET_CHUNK_VALUE,
                                PolarxResultset.Chunk.parseFrom(recvBuffer), packetSize));
                            break;

                        // TODO: others.
                        default:
                            XLog.XLogLogger.error(this + " Unknown ServerMessages.Type: " + type);
                            break;
                        }
                    } catch (Exception e) {
                        // Decode error.
                        XLog.XLogLogger.error(
                            this + " Bad packet!!! sid: " + sid + " type: " + type + " len: " + packetSize);
                        XLog.XLogLogger.error(e);
                        // Caution: Single decode error is ignored.
                        // This prevent impact on other session with same client.
                    } finally {
                        recvBuffer.position(nextPos);
                        recvBuffer.limit(oldLimit);
                    }
                } else if (XPacket.HEADER_SIZE + packetSize > recvBuffer.capacity()) {
                    // Larger buffer needed.
                    final ByteBuffer newBuf = ByteBuffer.allocate(packetSize + 0x1000)
                        .order(ByteOrder.LITTLE_ENDIAN); // On heap.
                    newBuf.put(recvBuffer);
                    recvBuffer.clear();
                    bigReadBuffer = newBuf;
                    XLog.XLogLogger.info(this + " switch to big buf.");
                    break;
                } else {
                    break;
                }
            }

            // Restore buffer.
            if (recvBuffer.hasRemaining()) {
                recvBuffer.compact(); // Prepare for next recv.
            } else {
                // No data.
                if (recvBuffer == bigReadBuffer) {
                    if (bigBufferClean.compareAndSet(true, false)) {
                        bigReadBuffer = null; // Free big buffer.
                        XLog.XLogLogger.info(this + " free big buf.");
                    } else {
                        bigReadBuffer.clear(); // Continue reuse big buffer.
                    }
                }
                readBuffer.clear();
            }
        } finally {
            readLock.unlock();
        }

        if (!batch.isEmpty()) {
            // Record correct packet.
            if (client != null) {
                // Record on per TCP and DN gather.
                client.getPerfCollection().getRecvMsgCount().getAndAdd(batch.size());
                client.getPool().getPerfCollection().getRecvMsgCount().getAndAdd(batch.size());
            }
            consumer.accept(batch);
        }
    }

    public void write(XPacket packet, boolean flush) throws IOException {
        final GeneratedMessageV3 msg;
        if (packet.getPacket() instanceof GeneratedMessageV3) {
            msg = (GeneratedMessageV3) packet.getPacket();
        } else {
            throw new TddlRuntimeException(
                com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_BAD_PACKET,
                this + " send unknown packet.");
        }
        final int size = msg.getSerializedSize();
        if (size > XConnectionManager.getInstance().getMaxPacketSize()) {
            throw new TddlRuntimeException(
                com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_BAD_PACKET,
                this + " sent packet length (" + size + ") "
                    + "exceeds the allowed maximum (" + XConnectionManager.getInstance().getMaxPacketSize() + ").");
        }

        final int fullSize = XPacket.HEADER_SIZE + size;

        Throwable throwable = null;
        final boolean directWrite = XConnectionManager.getInstance().isEnableDirectWrite();
        boolean needQueuedFlush = false;
        writeLock.lock();
        try {
            // To prevent reallocate after close.
            if (isClosed.get()) {
                throw new TddlRuntimeException(
                    com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_X_PROTOCOL_CLIENT,
                    this + " closed.");
            }

            boolean done = false;
            if (lastWrite != null) {
                final int limit = lastWrite.limit();
                if (limit + fullSize <= lastWrite.capacity()) {
                    // Capacity enough and append to tail.
                    lastWrite.position(limit);
                    lastWrite.limit(lastWrite.capacity());
                    lastWrite.putLong(packet.getSid());
                    if (XConfig.GALAXY_X_PROTOCOL) {
                        lastWrite.put(XPacket.VERSION);
                    }
                    lastWrite.putInt(XPacket.TYPE_SIZE + size);
                    lastWrite.put((byte) packet.getType());
                    msg.writeTo(CodedOutputStream.newInstance(lastWrite));
                    lastWrite.position(lastWrite.position() + size);
                    lastWrite.flip();
                    done = true;
                    // Skip to next if not enough.
                    if (lastWrite.limit() + XPacket.HEADER_SIZE > lastWrite.capacity()) {
                        lastWrite = null;
                    }
                }
            }

            if (!done) {
                // Get new buffer.
                final ByteBuffer buffer =
                    fullSize > processor.getBufferPool().getChunkSize() ?
                        ByteBuffer.allocate(fullSize).order(ByteOrder.LITTLE_ENDIAN) :
                        processor.getBufferPool().allocate();
                try {
                    buffer.putLong(packet.getSid());
                    if (XConfig.GALAXY_X_PROTOCOL) {
                        buffer.put(XPacket.VERSION);
                    }
                    buffer.putInt(XPacket.TYPE_SIZE + size);
                    buffer.put((byte) packet.getType());
                    msg.writeTo(CodedOutputStream.newInstance(buffer));
                    buffer.position(buffer.position() + size);
                    if (buffer.remaining() >= XPacket.HEADER_SIZE) {
                        lastWrite = buffer;
                    } else {
                        lastWrite = null;
                    }
                    buffer.flip();
                    writeQueue.offer(buffer);
                } catch (Throwable e) {
                    processor.getBufferPool().recycle(buffer);
                    throw e;
                }
            }

            if (directWrite) {
                if (flush) {
                    needQueuedFlush = !write0();
                }
            } else {
                needQueuedFlush = flush;
            }
        } catch (Throwable e) {
            throwable = e;
        } finally {
            writeLock.unlock();
        }

        if (throwable != null) {
            // Call cb out of lock.
            XLog.XLogLogger.error(throwable);
            fatalCallback.accept(throwable); // Any exception in write buffer may cause corruption on protocol.
            throw GeneralUtil.nestedException(throwable);
        }

        // Update msg count.
        if (client != null) {
            // Record on per TCP and DN gather.
            client.getPerfCollection().getSendMsgCount().getAndIncrement();
            client.getPool().getPerfCollection().getSendMsgCount().getAndIncrement();
        }

        // Flush if needed.
        if (needQueuedFlush) {
            flush();
        }
    }

    public void flush() {
        processor.postWrite(this);
    }

    // Must hold the write lock.
    private boolean write0() throws IOException {
        ByteBuffer top;
        while ((top = writeQueue.peek()) != null) {
            int written = channel.write(top);

            // Record perf data.
            if (written > 0 && client != null) {
                // Record on per TCP and DN gather.
                client.getPerfCollection().getSendFlushCount().getAndIncrement();
                client.getPerfCollection().getSendSize().getAndAdd(written);
                client.getPool().getPerfCollection().getSendFlushCount().getAndIncrement();
                client.getPool().getPerfCollection().getSendSize().getAndAdd(written);
            }

            if (top.hasRemaining()) {
                return false;
            } else {
                if (lastWrite == top) {
                    lastWrite = null; // Clear last.
                }
                final ByteBuffer removed = writeQueue.remove();
                assert removed == top;
                processor.getBufferPool().recycle(removed);
            }
        }
        return true;
    }

    private void enableWrite() {
        keyLock.lock();
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } finally {
            keyLock.unlock();
        }
        processKey.selector().wakeup();
    }

    private void disableWrite() {
        keyLock.lock();
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        } finally {
            keyLock.unlock();
        }
    }

    @Override
    public void writeByQueue() throws IOException {
        if (isClosed.get()) {
            return;
        }
        writeLock.lock();
        try {
            if ((processKey.interestOps() & SelectionKey.OP_WRITE) == 0 && !write0()) {
                // Write not blocking then try direct write and has remaining.
                enableWrite();
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void writeByEvent() throws IOException {
        if (isClosed.get()) {
            return;
        }
        writeLock.lock();
        try {
            if (write0()) {
                // All data written.
                disableWrite();
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void handleError(ErrorCode errCode, Throwable t) {
        XLog.XLogLogger.error(t);
        fatalCallback.accept(t);
    }

    public String getTag() {
        final SocketChannel c = channel;
        if (null == c) {
            return null;
        } else {
            try {
                return c.getLocalAddress().toString() + " -> " + c.getRemoteAddress().toString();
            } catch (Throwable ignore) {
                return null;
            }
        }
    }

    @Override
    public String toString() {
        final SocketChannel c = channel;
        if (null == c) {
            return "X-NIO-Client";
        } else {
            try {
                return "X-NIO-Client " + c.getLocalAddress().toString() + " to " + c.getRemoteAddress().toString();
            } catch (Throwable ignore) {
                return "X-NIO-Client";
            }
        }
    }
}
