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

package org.apache.hadoop.fs.nfs;


public class StreamStatistics {
  /* Name to identify this stream */
  private final String name;
  private final long streamID;

  /* Keyword for grep */
  private final static String KEYWORD = "STREAMSTATS";

  /* Total read/write bytes */
  private long bytesRead = 0;
  private long bytesWritten = 0;

  /* Total read/write operations */
  private int readOps = 0;
  private int largeReadOps = 0;
  private int writeOps = 0;

  /* Total read/write time in milliseconds */
  private long timeRead = 0;
  private long timeWritten = 0;

  /* Total NFS read/write bytes */
  private long bytesNFSRead = 0;
  private long bytesNFSWritten = 0;

  /* Number of NFS read/write operations */
  private int NFSReadOps = 0;
  private int NFSWriteOps = 0;

  /* read/write time spent in fetching/sending data to NFS server. */
  private long timeNFSRead = 0;
  private long timeNFSWritten = 0;

  /* Flag to tell whether it is a read or write stream. */
  private Boolean isRead;

  public StreamStatistics(String name, long streamID, Boolean isRead) {
    this.name = name;
    this.streamID = streamID;
    this.isRead = isRead;
  }

  public void incrementBytesRead(long bytesRead) {
    this.bytesRead += bytesRead;
  }

  public void incrementBytesWritten(long bytesWritten) {
    this.bytesWritten += bytesWritten;
  }

  public void incrementReadOps(int readOps) {
    this.readOps += readOps;
  }

  public void incrementWriteOps(int writeOps) {
    this.writeOps += writeOps;
  }

  public void incrementTimeRead(long timeRead) {
    this.timeRead += timeRead;
  }

  public void incrementTimeWritten(long timeWritten) {
    this.timeWritten += timeWritten;
  }

  public void incrementBytesNFSRead(long bytesNFSRead) {
    this.bytesNFSRead += bytesNFSRead;
  }

  public void incrementBytesNFSWritten(long bytesNFSWritten) {
    this.bytesNFSWritten += bytesNFSWritten;
  }

  public void incrementNFSReadOps(int NFSReadOps) {
    this.NFSReadOps += NFSReadOps;
  }

  public void incrementNFSWriteOps(int NFSWriteOps) {
    this.NFSWriteOps += NFSWriteOps;
  }

  public void incrementTimeNFSRead(long timeNFSRead) {
    this.timeNFSRead += timeNFSRead;
  }

  public void incrementTimeNFSWritten(long timeNFSWritten) {
    this.timeNFSWritten += timeNFSWritten;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(KEYWORD).append("streamStatistics: ").append("\n");
    buffer.append(KEYWORD).append("\tname: ").append(name).append("\n");
    buffer.append(KEYWORD).append("\tstreamID: ").append(streamID).append("\n");
    String STR_FORMAT = "%.3f";

    if (isRead) {
      /* stream statistics */
      buffer.append(KEYWORD).append("\t====InputStream Statistics====\n");
      buffer.append(KEYWORD).append("\tBytesRead: ").append(bytesRead)
          .append("\n");
      buffer.append(KEYWORD).append("\treadOps: ").append(readOps).append("\n");
      buffer.append(KEYWORD).append("\ttimeRead: ").append(timeRead / 1000.0)
          .append(" s\n");

      /* NFS write statistics */
      buffer.append(KEYWORD).append("\t====NFS Read Statistics====\n");
      buffer.append(KEYWORD).append("\tBytesNFSRead: ").append(bytesNFSRead)
          .append("\n");
      buffer.append(KEYWORD).append("\tNFSReadOps: ").append(NFSReadOps)
          .append("\n");
      buffer.append(KEYWORD).append("\ttimeNFSRead: ")
          .append(timeNFSRead / 1000.0).append(" s\n");

      /* Bandwidth for each operation. */
      buffer.append(KEYWORD).append("\t====Bandwidth====\n");
      buffer
          .append(KEYWORD)
          .append("\tRead: ")
          .append(
              String.format(STR_FORMAT, (double) bytesRead / 1024 / 1024
                  / (timeRead / 1000.0))).append(" MB/s\n");
      buffer
          .append(KEYWORD)
          .append("\tNFSRead: ")
          .append(
              String.format(STR_FORMAT, (double) bytesNFSRead / 1024 / 1024
                  / (timeNFSRead / 1000.0))).append(" MB/s\n");

      /* Average latency for each operation. */
      buffer.append(KEYWORD).append("\t====Average Latency====\n");
      buffer.append(KEYWORD).append("\tRead: ")
          .append(String.format(STR_FORMAT, timeRead / (double) readOps))
          .append(" ms\n");
      buffer.append(KEYWORD).append("\tNFSRead: ")
          .append(String.format(STR_FORMAT, timeNFSRead / (double) NFSReadOps))
          .append(" ms\n");

    } else {
      /* stream statistics */
      buffer.append(KEYWORD).append("\t====OutputStream Statistics====\n");
      buffer.append(KEYWORD).append("\tBytesWritten: ").append(bytesWritten)
          .append("\n");
      buffer.append(KEYWORD).append("\twriteOps: ").append(writeOps)
          .append("\n");
      buffer.append(KEYWORD).append("\ttimeWritten: ")
          .append(timeWritten / 1000.0).append(" s\n");

      /* NFS write statistics */
      buffer.append(KEYWORD).append("\t====NFS Write Statistics====\n");
      buffer.append(KEYWORD).append("\tBytesNFSWritten: ")
          .append(bytesNFSWritten).append("\n");
      buffer.append(KEYWORD).append("\tNFSWriteOps: ").append(NFSWriteOps)
          .append("\n");
      buffer.append(KEYWORD).append("\ttimeNFSWritten: ")
          .append(timeNFSWritten / 1000.0).append(" s\n");

      /* Bandwidth for each operation. */
      buffer.append(KEYWORD).append("\t====Bandwidth====\n");
      buffer
          .append(KEYWORD)
          .append("\tWrite: ")
          .append(
              String.format(STR_FORMAT, (double) bytesWritten / 1024 / 1024
                  / (timeWritten / 1000.0))).append(" MB/s\n");
      buffer
          .append(KEYWORD)
          .append("\tNFSWrite: ")
          .append(
              String.format(STR_FORMAT, (double) bytesNFSWritten / 1024 / 1024
                  / (timeNFSWritten / 1000.0))).append(" MB/s\n");

      /* Average latency for each operation. */
      buffer.append(KEYWORD).append("\t====Average Latency====\n");
      buffer.append(KEYWORD).append("\tWrite: ")
          .append(String.format(STR_FORMAT, timeWritten / (double) writeOps))
          .append(" ms\n");
      buffer
          .append(KEYWORD)
          .append("\tNFSWrite: ")
          .append(
              String.format(STR_FORMAT, timeNFSWritten / (double) NFSWriteOps))
          .append(" ms\n");

    }

    return buffer.toString();
  }
}
