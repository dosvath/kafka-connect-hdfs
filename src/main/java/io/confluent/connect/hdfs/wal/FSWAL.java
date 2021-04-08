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

  protected WALFile.Writer writer = null;
  private WALFile.Reader reader = null;
  private String logFile = null;
  private HdfsSinkConnectorConfig conf = null;
  private HdfsStorage storage = null;

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
    long sleepIntervalMs = WALConstants.INITIAL_SLEEP_INTERVAL_MS;
    while (sleepIntervalMs < WALConstants.MAX_SLEEP_INTERVAL_MS) {
      try {
        if (writer == null) {
          writer = WALFile.createWriter(conf, Writer.file(new Path(logFile)),
                                        Writer.appendIfExists(true));
          log.info(
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
  public void apply() throws ConnectException {
    log.debug("Starting to apply WAL");
    if (!storage.exists(logFile)) {
      log.debug("WAL file does not exist");
      return;
    }
    acquireLease();
    log.debug("Lease acquired");

    try {
      if (reader == null) {
        reader = new WALFile.Reader(conf.getHadoopConfiguration(), Reader.file(new Path(logFile)));
      }
      long latestOffset = commitWALEntriesToStorage();
      log.info("LATEST WAL OFFSET: {}", latestOffset);
    } catch (CorruptWalFileException e) {
      log.error("Error applying WAL file '{}' because it is corrupted: {}", logFile, e);
      log.warn("Truncating and skipping corrupt WAL file '{}'.", logFile);
      close();
    } catch (IOException e) {
      log.error("Error applying WAL file: {}, {}", logFile, e);
      close();
      throw new DataException(e);
    }
  }

  /**
   * Read all the filepath entries in the WAL file, commit the pending ones to HdfsStorage,
   * and return the offsets of the latest entry.
   *
   * @return the latest offsets commited to the filesystem based on the WAL file
   * @throws IOException when the WAL reader is unable to get the next entry
   */
  private long commitWALEntriesToStorage() throws IOException {
    Map<WALEntry, WALEntry> entries = new HashMap<>();
    WALEntry key = new WALEntry();
    WALEntry value = new WALEntry();

    // The entry with the latest offsets is the one after the BEGIN marker
    // in the last BEGIN-END block of the file.
    // the committed filepath entry with the latest offsets
    String latestEntry = null;
    // whether the WAL entry was a BEGIN marker
    boolean wasBeginMarker = false;

    while (reader.next(key, value)) {
      log.debug("\ncurrent k name: {}\n current v name: {}", key.getName(), value.getName());
      String keyName = key.getName();

      if (keyName.equals(beginMarker)) {
        log.debug("key is BEGIN marker: {}", beginMarker);
        entries.clear();
        wasBeginMarker = true;
      } else if (keyName.equals(endMarker)) {
        log.debug("key is END marker: {}", endMarker);
        commitEntriesToStorage(entries);
      } else {
        log.debug("key neither BEGIN nor END marker");
        WALEntry mapKey = new WALEntry(key.getName());
        WALEntry mapValue = new WALEntry(value.getName());
        log.debug("adding (K,V) to entries hashmap: \n K: {}\n V: {})",
            key.getName(), value.getName());
        entries.put(mapKey, mapValue);
        if (wasBeginMarker) {
          latestEntry = value.getName();
          wasBeginMarker = false;
        }
      }
    }

    log.debug("Finished applying WAL");
    return extractOffsetsFromFilePath(latestEntry);
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
    if (fullPath != null) {
      String latestFileName = Paths.get(fullPath).getFileName().toString();
      return FileUtils.extractOffset(latestFileName);
    }
    return -1;
  }

  @Override
  public void truncate() throws ConnectException {
    try {
      String oldLogFile = logFile + ".1";
      if (storage.exists(oldLogFile)) {
        log.info("found old log file: {}", oldLogFile);
      }
      storage.delete(oldLogFile);
      storage.commit(logFile, oldLogFile);
    } finally {
      close();
    }
  }

  @Override
  public void close() throws ConnectException {
    log.info(
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
