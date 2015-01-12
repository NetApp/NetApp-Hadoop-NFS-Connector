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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.nfs.NFSv3FileSystemStore;
import org.apache.hadoop.fs.nfs.StreamStatistics;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;

public class NFSBufferedInputStream extends FSInputStream {

  long fileLength;
  long fileOffset;
  long prefetchBlockLimit;
  StreamBlock currentBlock;

  final NFSv3FileSystemStore store;
  final FileHandle handle;

  final String pathString;
  final int readBlockSizeBits;
  final long splitSize;
  final StreamStatistics statistics;
  final int poolSize;
  final boolean doPrefetch;
  final AtomicBoolean closed;
  final ExecutorService executors;
  final Map<Long, Future<Read>> ongoing;
  final Map<Long, StreamBlock> cache;

  static final AtomicInteger streamId;

  public static final int DEFAULT_CACHE_SIZE_IN_BLOCKS = 512;

  public static final int MIN_PREFETCH_POOL_SIZE = 1;
  public static final int MAX_PREFETCH_POOL_SIZE = 512;
  public static final int DEFAULT_PREFETCH_POOL_SIZE = 128;
  public static final boolean DEFAULT_PREFETCH_ENABLED = true;

  public static final int DEFAULT_READAHEAD_SIZE = 128;

  public final static Log LOG = LogFactory.getLog(NFSBufferedInputStream.class);

  static {
    streamId = new AtomicInteger(1);
  }

  public NFSBufferedInputStream(Configuration configuration, NFSv3FileSystemStore store,
      FileHandle handle, Path f, Configuration conf, int readBlockSizeBits, String scheme,
      long splitSize, FileSystem.Statistics fsStat) throws IOException {

    this.store = store;
    this.handle = handle;
    this.pathString = f.toUri().getPath();


    poolSize =
        Math.min(
            MAX_PREFETCH_POOL_SIZE,
            Math.max(MIN_PREFETCH_POOL_SIZE,
                conf.getInt("fs.nfs.numprefetchthreads", DEFAULT_PREFETCH_POOL_SIZE)));
    doPrefetch = conf.getBoolean("fs.nfs.prefetch", DEFAULT_PREFETCH_ENABLED);


    this.fileOffset = 0L;
    this.readBlockSizeBits = readBlockSizeBits;
    this.splitSize = splitSize;
    this.closed = new AtomicBoolean(false);
    this.ongoing = new ConcurrentHashMap<Long, Future<Read>>(poolSize);
    this.cache = new ConcurrentHashMap<Long, StreamBlock>(DEFAULT_CACHE_SIZE_IN_BLOCKS);
    this.statistics =
        new StreamStatistics(NFSBufferedInputStream.class + pathString, streamId.getAndIncrement(),
            true);
    this.executors = new ThreadPoolExecutor(32, poolSize, 1, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>(1024), new ThreadPoolExecutor.CallerRunsPolicy());

    // Keep track of the file length at file open
    // NOTE: The file does not get modified while this stream is open
    Nfs3FileAttributes attributes = store.getFileAttributes(handle, store.getCredentials());
    if (attributes != null) {
      this.fileLength = attributes.getSize();
      if (this.fileLength < 0) {
        throw new IOException("File length is invalid: " + this.fileLength);
      }
    } else {
      throw new IOException("Could not get file length from NFS server");
    }

  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos > fileLength) {
      throw new IOException("Cannot seek after EOF: pos=" + pos + ", fileLength=" + fileLength);
    }
    fileOffset = pos;
    prefetchBlockLimit = (long) (Math.min(fileLength, pos + this.splitSize) >> readBlockSizeBits);
  }

  @Override
  public synchronized long getPos() throws IOException {
    return fileOffset;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] data = new byte[1];
    read(data, 0, 1);
    return (int) data[0];
  }

  @Override
  public synchronized int read(byte data[]) throws IOException {
    return read(data, 0, data.length);
  }

  @Override
  public synchronized int read(byte data[], int offset, int length) throws IOException {
    long enterTime = System.currentTimeMillis();
    int bytesRead = -1;

    try {
      bytesRead = _read(data, offset, length);
    } finally {
      if (bytesRead >= 0) {
        statistics.incrementBytesRead(bytesRead);
        statistics.incrementReadOps(1);
        statistics.incrementTimeRead(System.currentTimeMillis() - enterTime);
      }
    }
    return bytesRead;
  }

  private synchronized int _read(byte data[], int offset, int length) throws IOException {

    int lengthToRead = Math.min(data.length, length);
    int blockSize = (int) (1 << readBlockSizeBits);
    long loBlockId = (long) (fileOffset >> readBlockSizeBits);
    long hiBlockId = (long) ((fileOffset + lengthToRead - 1) >> readBlockSizeBits);
    int loOffset = (int) (fileOffset - (loBlockId << readBlockSizeBits));
    int hiOffset = (int) ((fileOffset + lengthToRead - 1) - (hiBlockId << readBlockSizeBits));

    if(closed.get()) {
      LOG.warn("Reading from an already closed InputStream. Check your code");
    }

    if (loBlockId == hiBlockId) {
      StreamBlock block = getBlock(loBlockId);
      if (block == null) {
        return -1;
      } else {
        int bytesRead = block.readFromBlock(data, offset, loOffset, lengthToRead);
        if (bytesRead != -1) {
          fileOffset += bytesRead;
        }
        return bytesRead;
      }
    } else {
      int totalBytesRead = offset;
      for (long blk = loBlockId; blk <= hiBlockId; blk++) {
        StreamBlock block = getBlock(blk);
        if (block == null) {
          if (blk == loBlockId) {
            return -1;
          } else {
            return (totalBytesRead - offset);
          }
        }

        if (blk == loBlockId) {
          int bytesRead = block.readFromBlock(data, totalBytesRead, loOffset, blockSize - loOffset);
          if (bytesRead == -1) {
            return -1;
          }
          totalBytesRead += bytesRead;
          fileOffset += bytesRead;
        } else if (blk == hiBlockId) {
          int bytesRead = block.readFromBlock(data, totalBytesRead, 0, hiOffset + 1);
          if (bytesRead != -1) {
            totalBytesRead += bytesRead;
            fileOffset += bytesRead;
          }
        } else {
          int bytesRead = block.readFromBlock(data, totalBytesRead, 0, blockSize);
          if (bytesRead != -1) {
            totalBytesRead += bytesRead;
            fileOffset += bytesRead;
          } else {
            break;
          }
        }
      }
      return (totalBytesRead - offset);
    }
  }

  private StreamBlock getBlock(long blockId) throws IOException {

    // Block is current
    if (currentBlock != null && currentBlock.getBlockId() == blockId) {
      LOG.debug("Returning 1 blockId=" + blockId);
      return currentBlock;
    }

    // Issue prefetch for upcoming blocks
    if (doPrefetch) {
      for (long bid = blockId + 1; bid < blockId + DEFAULT_READAHEAD_SIZE; ++bid) {
        if (!ongoing.containsKey(bid) && !cache.containsKey(bid)) {
          StreamBlock block = new StreamBlock(readBlockSizeBits);
          block.setBlockId(bid);
          block.setReady(false);
          cache.put(bid, block);

          Read task = new Read(store, handle, statistics, bid, block);
          Future<Read> future = executors.submit(task);
          ongoing.put(bid, future);
        }
      }
    }

    // Block is being fetched, so wait for it
    if (ongoing.containsKey(blockId)) {
      Future<Read> future = ongoing.get(blockId);
      while(true) {
        try {
          LOG.debug("Waiting for read task to complete ongoing reading block id=" + blockId);
          future.get();
          break;
        } catch(InterruptedException interrupted) {
          continue;
        } catch(Exception error) {
          throw new IOException("Read resulted in an error", error);
        }
      }
    }

    // Some prefetches are done, check for them
    for (Iterator<Entry<Long, Future<Read>>> iter = ongoing.entrySet().iterator(); iter.hasNext();) {
      Future<Read> future = iter.next().getValue();
      if(future.isDone()) {
        try {
          future.get();
          iter.remove();
        } catch(InterruptedException interrupted) {
          // Ignore
        } catch(Exception error) {
          throw new IOException("Prefetched resulted in error", error);
        }
      }
    }

    // Keep trying until the block is found
    while (true) {

      if (cache.containsKey(blockId)) {
        StreamBlock block = cache.remove(blockId);
        assert (block != null);
        assert (block.getBlockId() == blockId);
        assert (block.isReady() == true);
        currentBlock = block;
        return currentBlock;
      }

      if (cache.size() >= 100 * DEFAULT_CACHE_SIZE_IN_BLOCKS) {
        LOG.error("Cache is wayyy bigger than planned, size=" + cache.size() + " limit="
            + DEFAULT_CACHE_SIZE_IN_BLOCKS);
      }

      // Issue the read and wait
      StreamBlock block = new StreamBlock(readBlockSizeBits);
      block.setBlockId(blockId);
      block.setReady(false);
      cache.put(blockId, block);

      Read task = new Read(store, handle, statistics, blockId, block);
      Future<Read> future = executors.submit(task);
      while(true) {
        try {
          future.get();
          break;
        } catch(InterruptedException interrupted) {
          continue;
        } catch(Exception error) {
          throw new IOException("Read resulted in an error", error);
        }
      }
      LOG.info("Looping inside while loop");
    }

  }

  @Override
  public void close() throws IOException {

    boolean first = true;
    if(closed.get()) {
      first = false;
      LOG.warn("Closing an already closed InputStream. Check your code");
    }
    closed.set(true);

    // Shutdown the thread pool
    if(first) {
      executors.shutdown();
      try {
        executors.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException exception) {
        // Ignore
      }
    }
    
    LOG.info(statistics);
    super.close();
  }

}
