/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.wal;

import java.nio.file.Paths;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.wal.WALFile.Reader;
import io.confluent.connect.hdfs.wal.WALFile.Writer;

public class FSWAL implements WAL {

  private static final Logger log = LoggerFactory.getLogger(FSWAL.class);
  private static final String OLD_LOG_EXTENSION = ".1";

  private final HdfsSinkConnectorConfig conf;
  private final HdfsStorage storage;
  private final String logFile;

  protected WALFile.Writer writer = null;
  private WALFile.Reader reader = null;

  public FSWAL(String logsDir, TopicPartition topicPart, HdfsStorage storage)
      throws ConnectException {
    this.storage = storage;
    this.conf = storage.conf();
    String url = storage.url();
    logFile = FileUtils.logFileName(url, logsDir, topicPart);
  }

  @Override
  public void append(String tempFile, String committedFile) throws ConnectException {
    try {
      acquireLease();
      WALEntry key = new WALEntry(tempFile);
      WALEntry value = new WALEntry(committedFile);
      writer.append(key, value);
      writer.hsync();
    } catch (IOException e) {
      log.error("Error appending WAL file: {}, {}", logFile, e);
      close();
      throw new DataException(e);
    }
  }

  public void acquireLease() throws ConnectException {
    log.debug("Attempting to acquire lease for WAL file: {}", logFile);
    long sleepIntervalMs = WALConstants.INITIAL_SLEEP_INTERVAL_MS;
    while (sleepIntervalMs < WALConstants.MAX_SLEEP_INTERVAL_MS) {
      try {
        if (writer == null) {
          writer = WALFile.createWriter(conf, Writer.file(new Path(logFile)),
                                        Writer.appendIfExists(true));
          log.debug(
              "Successfully acquired lease, {}-{}, file {}",
              conf.name(),
              conf.getTaskId(),
              logFile
          );
        }
        break;
      } catch (RemoteException e) {
        if (e.getClassName().equals(WALConstants.LEASE_EXCEPTION_CLASS_NAME)) {
          log.warn(
              "Cannot acquire lease on WAL, {}-{}, file {}",
              conf.name(),
              conf.getTaskId(),
              logFile
          );
          try {
            Thread.sleep(sleepIntervalMs);
          } catch (InterruptedException ie) {
            throw new ConnectException(ie);
          }
          sleepIntervalMs = sleepIntervalMs * 2;
        } else {
          throw new ConnectException(e);
        }
      } catch (IOException e) {
        throw new DataException(
            String.format(
                "Error creating writer for log file, %s-%s, file %s",
                conf.name(),
                conf.getTaskId(),
                logFile
            ),
            e
        );
      }
    }
    if (sleepIntervalMs >= WALConstants.MAX_SLEEP_INTERVAL_MS) {
      throw new ConnectException("Cannot acquire lease after timeout, will retry.");
    }
  }

  @Override
  public long apply() throws ConnectException {
    log.debug("Starting to apply WAL: {}", logFile);
    if (!storage.exists(logFile)) {
      log.debug("WAL file does not exist: {}", logFile);
      return -1;
    }
    acquireLease();
    log.debug("Lease acquired");

    try {
      if (reader == null) {
        reader = new WALFile.Reader(conf.getHadoopConfiguration(), Reader.file(new Path(logFile)));
      }
      return commitWalEntriesToStorage();
    } catch (CorruptWalFileException e) {
      log.error("Error applying WAL file '{}' because it is corrupted: {}", logFile, e);
      log.warn("Truncating and skipping corrupt WAL file '{}'.", logFile);
      close();
      return -1;
    } catch (IOException e) {
      log.error("Error applying WAL file: {}, {}", logFile, e);
      close();
      throw new ConnectException(e);
    }
  }

  /**
   * Read all the filepath entries in the WAL file, commit the pending ones to HdfsStorage
   *
   * @throws IOException when the WAL reader is unable to get the next entry
   */
  private long commitWalEntriesToStorage() throws IOException {
    Map<WALEntry, WALEntry> entries = new HashMap<>();
    WALEntry key = new WALEntry();
    WALEntry value = new WALEntry();
    // The entry with the latest offsets is the one after the BEGIN marker
    // in the last BEGIN-END block of the file. There may be empty BEGIN and END blocks as well.
    // the committed filepath entry with the latest offsets
    String latestEntry = null;
    // whether the WAL entry was a BEGIN marker
    boolean wasBeginMarker = false;

    while (reader.next(key, value)) {
      String keyName = key.getName();
      if (keyName.equals(beginMarker)) {
        entries.clear();
        wasBeginMarker = true;
      } else if (keyName.equals(endMarker)) {
        commitEntriesToStorage(entries);
      } else {
        WALEntry mapKey = new WALEntry(key.getName());
        WALEntry mapValue = new WALEntry(value.getName());
        entries.put(mapKey, mapValue);
        if (wasBeginMarker) {
          latestEntry = value.getName();
          wasBeginMarker = false;
        }
      }
    }
    log.debug("Finished applying WAL: {}", logFile);
    long latestOffset = extractOffsetsFromFilePath(latestEntry);
    log.trace("Latest offset from WAL: {}", latestOffset);
    return latestOffset;
  }

  /**
   * Commit the given WAL file entries to HDFS storage,
   * typically a batch between BEGIN and END markers in the WAL file.
   *
   * @param entries a map of filepath entries containing temp and committed paths
   */
  private void commitEntriesToStorage(Map<WALEntry, WALEntry> entries) {
    for (Map.Entry<WALEntry, WALEntry> entry: entries.entrySet()) {
      String tempFile = entry.getKey().getName();
      String committedFile = entry.getValue().getName();
      if (!storage.exists(committedFile)) {
        storage.commit(tempFile, committedFile);
      }
    }
  }

  /**
   * Extract the file offset from the full file path.
   *
   * @param fullPath the full HDFS file path
   * @return the offset or -1 if not present
   */
  private long extractOffsetsFromFilePath(String fullPath) {
    try {
      if (fullPath != null) {
        String latestFileName = Paths.get(fullPath).getFileName().toString();
        return FileUtils.extractOffset(latestFileName);
      }
    } catch (IllegalArgumentException e) {
      log.warn("Could not extract offsets from file path: {}", fullPath);
    }
    return -1;
  }

  /**
   * Recover the latest offsets from the old WAL file. This is needed
   * when the most recent WAL file has already been truncated and is not existent.
   * The above may happen when the connector has flushed all records, and is restarted.
   *
   * @return the latest offsets from the old WAL file
   */
  public long recoverOffsetsFromOldLog() {
    String oldFilePath = logFile + OLD_LOG_EXTENSION;
    if (storage.exists(oldFilePath)) {
      log.trace("Recovering offsets from old WAL file {}", oldFilePath);
      Reader oldFileReader = null;
      try {
        oldFileReader =
            new WALFile.Reader(conf.getHadoopConfiguration(), Reader.file(new Path(oldFilePath)));
        long latestOffset = extractOffsetsFromFilePath(getLatestEntry(oldFileReader));
        log.trace("Latest offset from old WAL: {}", latestOffset);
        return latestOffset;
      } catch (IOException e) {
        log.warn("Error recovering offsets from old WAL file: {}, {}", oldFilePath, e.getMessage());
      } finally {
        if (oldFileReader != null) {
          try {
            oldFileReader.close();
            log.trace("Closed old WAL reader.");
          } catch (IOException e) {
            log.warn("Error closing old WAL file reader: {}, {}", oldFilePath, e.getMessage());
          }
        }
      }
    }

    return -1;
  }

  /**
   * Get the latest entry from the old WAL file that contains the latest offsets written to HDFS.
   *
   * @param oldFileReader the reader for the old WAL log file
   * @return the latest value entry filepath
   * @throws IOException if the WAL cannot be read
   */
  private String getLatestEntry(Reader oldFileReader) throws IOException {
    WALEntry key = new WALEntry();
    WALEntry value = new WALEntry();
    // The entry with the latest offsets is the one after the BEGIN marker
    // in the last BEGIN-END block of the file. There may be empty BEGIN and END blocks as well.
    // the committed filepath entry with the latest offsets
    String latestEntry = null;
    // whether the WAL entry was a BEGIN marker
    boolean wasBeginMarker = false;

    while (oldFileReader.next(key, value)) {
      String keyName = key.getName();
      if (keyName.equals(beginMarker)) {
        wasBeginMarker = true;
      } else if (!keyName.equals(endMarker)) {
        if (wasBeginMarker) {
          latestEntry = value.getName();
          wasBeginMarker = false;
        }
      }
    }
    return latestEntry;
  }

  @Override
  public void truncate() throws ConnectException {
    log.debug("Truncating WAL file: {}", logFile);
    try {
      String oldLogFile = logFile + OLD_LOG_EXTENSION;
      storage.delete(oldLogFile);
      storage.commit(logFile, oldLogFile);
    } finally {
      close();
    }
  }

  @Override
  public void close() throws ConnectException {
    log.debug(
        "Closing WAL, {}-{}, file: {}",
        conf.name(),
        conf.getTaskId(),
        logFile
    );
    try {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new DataException("Error closing " + logFile, e);
    } finally {
      writer = null;
      reader = null;
    }
  }

  @Override
  public String getLogFile() {
    return logFile;
  }
}
