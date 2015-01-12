/**
 * Copyright 2014 NetApp Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.nfs.stream;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StreamBlock {

  private long id;
  private int dataLength;
  private int startOffset;
  private final int key;
  private final byte[] array;
  private final int blockSizeBits;
  private final AtomicBoolean ready;

  public static final int ARRAY_COPY_THRESHOLD = 128;

  public static final AtomicInteger counter;
  public static final Log LOG = LogFactory.getLog(StreamBlock.class);

  static {
    counter = new AtomicInteger(1);
  }

  public StreamBlock(int blockSizeBits) {
    assert (blockSizeBits > 0 && blockSizeBits < 25);
    array = new byte[(1 << blockSizeBits)];
    this.blockSizeBits = blockSizeBits;
    this.startOffset = (1 << blockSizeBits);
    this.dataLength = 0;
    this.id = -1L;
    this.ready = new AtomicBoolean(false);
    key = counter.getAndIncrement();
  }

  public synchronized void reset() {
    this.startOffset = (1 << blockSizeBits);
    this.dataLength = 0;
  }

  public synchronized long getBlockId() {
    return id;
  }

  public synchronized void setBlockId(long blockId) {
    this.id = blockId;
  }

  public byte[] array() {
    return array;
  }

  public synchronized int readFromBlock(byte[] buffer, int offsetInBuffer,
      int offsetInBlock, int lengthToRead) throws IOException {

    int length = Math.min(lengthToRead, getDataLength() - offsetInBlock);

    // Check for errors
    if (buffer == null || array == null) {
      throw new IOException("Buffer(s) are null");
    }
    if (length < 0) {
      throw new IOException("Cannot write zero or less bytes");
    }
    if (length > buffer.length) {
      throw new IOException("Cannot write longer than input buffer size");
    }
    if (offsetInBlock < 0) {
      throw new IOException("Block offset is not within bounds");
    }
    if (offsetInBuffer < 0 || offsetInBuffer >= buffer.length) {
      throw new IOException("Buffer offset is not in bounds");
    }
    if (offsetInBlock >= getDataLength()) {
      return -1;
    }

    // Copy the data
    if (length <= ARRAY_COPY_THRESHOLD) {
      for (int i = 0; i < length; ++i) {
        buffer[offsetInBuffer + i] = array[offsetInBlock + i];
      }
    } else {
      System.arraycopy(array, offsetInBlock, buffer, offsetInBuffer, length);
    }

    return length;
  }

  public synchronized int writeToBlock(byte[] buffer, int offsetInBuffer,
      int offsetInBlock, int lengthToWrite) throws IOException {
    int length = Math.min(lengthToWrite, getBlockSize() - offsetInBlock);

    // Check for errors
    if (buffer == null || array == null) {
      throw new IOException("Buffer(s) are null");
    }
    if (length < 0) {
      throw new IOException("Cannot write zero or less bytes");
    }
    if (length > buffer.length) {
      throw new IOException("Cannot write longer than input buffer size");
    }
    if (offsetInBlock < 0 || offsetInBlock >= getBlockSize()) {
      throw new IOException("Block offset is not within bounds");
    }
    if (buffer.length > 0 && (offsetInBuffer < 0 || offsetInBuffer >= buffer.length)) {
      throw new IOException("Buffer offset is not in bounds: offsetInBuffer=" + offsetInBuffer + " buffer.length=" + buffer.length + " length=" + length);
    }

    // Copy the data
    if (length <= ARRAY_COPY_THRESHOLD) {
      for (int i = 0; i < length; ++i) {
        array[offsetInBlock + i] = buffer[offsetInBuffer + i];
      }
    } else {
      System.arraycopy(buffer, offsetInBuffer, array, offsetInBlock, length);
    }
    startOffset = Math.min(offsetInBlock, startOffset);
    dataLength = offsetInBlock + length - startOffset;

    return length;
  }

  public synchronized int getBlockSizeBits() {
    return blockSizeBits;
  }

  public synchronized int getBlockSize() {
    return (1 << blockSizeBits);
  }

  public synchronized int getDataStartOffset() {
    return startOffset;
  }

  public synchronized int getDataLength() {
    return dataLength;
  }

  public int getKey() {
    return key;
  }

  public boolean isReady() {
    return ready.get();
  }

  public boolean setReady(boolean isReady) {
    return ready.getAndSet(isReady);
  }

}
