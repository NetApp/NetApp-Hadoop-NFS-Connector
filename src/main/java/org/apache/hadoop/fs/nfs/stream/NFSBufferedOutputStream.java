/**
 * Copyright 2014 NetApp Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.nfs.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;
import org.apache.hadoop.fs.nfs.StreamStatistics;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.oncrpc.security.Credentials;

public class NFSBufferedOutputStream extends OutputStream {

  final FileHandle handle;
  final Credentials credentials;
  final Path path;
  final String pathString;
  final StreamStatistics statistics;
  final NFSv3FileSystemStore store;
  final AtomicBoolean closed;
  final int blockSizeBits;
  final ExecutorService executors;
  final List<Future<Write>> ongoing;

  long fileOffset;
  StreamBlock currentBlock;

  private static final int MAX_WRITEBACK_POOL_SIZE = 256;
  private static final int DEFAULT_WRITEBACK_POOL_SIZE = 4;

  static final AtomicInteger streamId;

  public final static Log LOG = LogFactory.getLog(NFSBufferedOutputStream.class);

  static {
    streamId = new AtomicInteger(1);
  }

  public NFSBufferedOutputStream(Configuration configuration, FileHandle handle, Path path,
      NFSv3FileSystemStore store, Credentials credentials, boolean append) throws IOException {

    this.handle = handle;
    this.credentials = credentials;
    this.path = path;
    this.pathString = path.toUri().getPath();
    this.statistics =
        new StreamStatistics(NFSBufferedInputStream.class + pathString, streamId.getAndIncrement(),
            false);
    this.store = store;
    this.blockSizeBits = store.getWriteSizeBits();
    this.currentBlock = null;
    this.closed = new AtomicBoolean(false);

    assert (blockSizeBits >= 0 && blockSizeBits <= 22);

    // Create the task queues
    executors =
        new ThreadPoolExecutor(DEFAULT_WRITEBACK_POOL_SIZE, MAX_WRITEBACK_POOL_SIZE, 5, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>(1024), new ThreadPoolExecutor.CallerRunsPolicy());
    ongoing = new LinkedList<>();

    // Set file offset to 0 or file length
    if (append) {
      Nfs3FileAttributes attributes = store.getFileAttributes(handle, credentials);
      if (attributes != null) {
        fileOffset = attributes.getSize();
        LOG.info("Appending to file so starting at offset = " + fileOffset);
      } else {
        throw new IOException("Could not get file length");
      }
    } else {
      fileOffset = 0L;
    }

  }

  @Override
  public void write(int b) throws IOException {
    byte buffer[] = new byte[1];
    buffer[0] = (byte) b;
    _write(buffer, 0, 1);
  }

  @Override
  public void write(byte[] data) throws IOException {
    _write(data, 0, data.length);
  }

  @Override
  public void write(byte[] data, int offset, int length) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      _write(data, offset, length);
    } finally {
      statistics.incrementBytesWritten(length);
      statistics.incrementWriteOps(1);
      statistics.incrementTimeWritten((System.currentTimeMillis() - startTime));
    }
  }

  private synchronized void _write(byte[] data, int offset, int length) throws IOException {

    int lengthToWrite = Math.min(data.length, length);
    int blockSize = (int) (1 << blockSizeBits);
    long loBlockId = (long) (fileOffset >> blockSizeBits);
    long hiBlockId = (long) ((fileOffset + lengthToWrite - 1) >> blockSizeBits);
    int loOffset = (int) (fileOffset - (loBlockId << blockSizeBits));
    int hiOffset = (int) ((fileOffset + lengthToWrite - 1) - (hiBlockId << blockSizeBits));

    if (closed.get() == true) {
      LOG.warn("Trying to write to a closed stream. Check your code.");
    }

    // All the data is in one block, so it's easy to handle
    if (loBlockId == hiBlockId) {
      StreamBlock block = getBlock(loBlockId);
      assert (block != null);

      int bytesWritten = block.writeToBlock(data, offset, loOffset, lengthToWrite);
      assert (bytesWritten == lengthToWrite);
      fileOffset += bytesWritten;
    }
    // The data is in multiple blocks, so we need do much more work
    else {
      int totalBytesWritten = offset;
      for (long blk = loBlockId; blk <= hiBlockId; blk++) {
        StreamBlock block = getBlock(blk);
        assert (block != null);

        // We write from loOffset in the starting block
        if (blk == loBlockId) {
          int bytesWritten = block.writeToBlock(data, offset, loOffset, blockSize - loOffset);
          assert (bytesWritten == (blockSize - loOffset));
          totalBytesWritten += bytesWritten;
          fileOffset += bytesWritten;
        }
        // We write up to hiOffset in the ending block
        else if (blk == hiBlockId) {
          int bytesWritten = block.writeToBlock(data, totalBytesWritten, 0, hiOffset + 1);
          assert (bytesWritten == (hiOffset + 1));
          totalBytesWritten += bytesWritten;
          fileOffset += bytesWritten;
        }
        // Middle blocks are written fully
        else {
          int bytesWritten = block.writeToBlock(data, totalBytesWritten, 0, blockSize);
          assert (bytesWritten == blockSize);
          totalBytesWritten += bytesWritten;
          fileOffset += bytesWritten;
        }
      }
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    try {

      if (closed.get() == true) {
        LOG.warn("Flushing a closed stream. Check your code.");
      }

      // Write back the current block
      if (currentBlock != null) {
        flushBlock(currentBlock);
      }

      // Commit all outstanding changes
      Commit commit = new Commit(this, store, handle, credentials, 0L, 0);
      Future<Commit> future = executors.submit(commit);
      while (true) {
        try {
          future.get();
          break;
        } catch (InterruptedException exception) {
          LOG.info("Got interrupted while waiting for COMMIT to finish, trying again");
          continue;
        }
      }
    } catch (Exception exception) {
      throw new IOException("Could not flush stream");
    }
  }

  @Override
  public synchronized void close() throws IOException {

    boolean first = true;
    long start = System.currentTimeMillis();

    if (closed.get() == true) {
      first = false;
      LOG.warn("Closing an already closed output stream");
    }
    closed.set(true);

    // Shutdown the thread pool
    if (first) {
      flush();
      executors.shutdown();
      try {
        executors.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException exception) {
        // Ignore
      }
    }

    LOG.info(statistics);
    super.close();
    LOG.info("OutputStream shutdown took " + (System.currentTimeMillis() - start) + " ms");
  }

  private StreamBlock getBlock(long blockId) throws IOException {
    if (currentBlock != null && blockId == currentBlock.getBlockId()) {
      return currentBlock;
    } else {
      if (currentBlock != null) {
        flushBlock(currentBlock);
      }
      currentBlock = new StreamBlock(blockSizeBits);
      currentBlock.setBlockId(blockId);
      return currentBlock;
    }
  }

  private void checkOngoing() throws IOException {
    if (ongoing.size() >= 64) {
      for (Iterator<Future<Write>> iter = ongoing.iterator(); iter.hasNext();) {
        Future<Write> f = iter.next();
        if (!f.isDone()) {
          try {
            f.get();
            iter.remove();
          } catch (InterruptedException interrupted) {
            // Ignore
          } catch (ExecutionException execution) {
            throw new IOException("Write back call failed", execution);
          }
        }
      }
    }
  }

  private void flushBlock(StreamBlock block) throws IOException {
    // Check ongoing
    checkOngoing();

    // Submit new task
    Write call = new Write(store, handle, credentials, statistics, block.getBlockId(), currentBlock);
    Future<Write> future = executors.submit(call);
    ongoing.add(future);
  }

}
