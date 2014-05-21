/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.crypto.CryptoModule;
import org.apache.accumulo.core.security.crypto.CryptoModuleFactory;
import org.apache.accumulo.core.security.crypto.CryptoModuleParameters;
import org.apache.accumulo.core.security.crypto.DefaultCryptoModule;
import org.apache.accumulo.core.security.crypto.NoFlushOutputStream;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.tserver.TabletMutations;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import com.google.common.base.Joiner;

/**
 * Wrap a connection to a logger.
 * 
 */
public class DfsLogger {
  // Package private so that LogSorter can find this
  static final String LOG_FILE_HEADER_V2 = "--- Log File Header (v2) ---";
  static final String LOG_FILE_HEADER_V3 = "--- Log File Header (v3) ---";
  
  static final String LOG_COMPRESSION_HEADER = "File Compressed";
  
  private static Logger log = Logger.getLogger(DfsLogger.class);
  

  public static class LogClosedException extends IOException {
    private static final long serialVersionUID = 1L;

    public LogClosedException() {
      super("LogClosed");
    }
  }

  public static class DFSLoggerInputStreams {

    private FSDataInputStream originalInput;
    private DataInputStream decryptingInputStream;

    public DFSLoggerInputStreams(FSDataInputStream originalInput, DataInputStream decryptingInputStream) {
      this.originalInput = originalInput;
      this.decryptingInputStream = decryptingInputStream;
    }

    public FSDataInputStream getOriginalInput() {
      return originalInput;
    }

    public void setOriginalInput(FSDataInputStream originalInput) {
      this.originalInput = originalInput;
    }

    public DataInputStream getDecryptingInputStream() {
      return decryptingInputStream;
    }

    public void setDecryptingInputStream(DataInputStream decryptingInputStream) {
      this.decryptingInputStream = decryptingInputStream;
    }
  }

  public interface ServerResources {
    AccumuloConfiguration getConfiguration();

    VolumeManager getFileSystem();

    Set<TServerInstance> getCurrentTServers();
  }

  private final LinkedBlockingQueue<DfsLogger.LogWork> workQueue = new LinkedBlockingQueue<DfsLogger.LogWork>();
  
  private final Object closeLock = new Object();

  private static final DfsLogger.LogWork CLOSED_MARKER = new DfsLogger.LogWork(null, false);

  private static final LogFileValue EMPTY = new LogFileValue();
  
  private boolean closed = false;
  private boolean useCompression;
  
  private Thread flushingThread;

  private class LogSyncingTask implements Runnable {

    @Override
    public void run() {
      ArrayList<DfsLogger.LogWork> work = new ArrayList<DfsLogger.LogWork>();
      
      boolean sawClosedMarker = false;
     
      while (true) {
        work.clear();
        
        
        try {
          work.add(workQueue.take());
        } catch (InterruptedException ex) {
          continue;
        }
        workQueue.drainTo(work);

        try {

          encryptingLogFile.flush();
          sync.invoke(logFile);

        } catch (Exception ex) {
          log.warn("Exception syncing " + ex);
          for (DfsLogger.LogWork logWork : work) {
            logWork.exception = ex;
          }
        }
       
        for (DfsLogger.LogWork logWork : work) {
          if (logWork == CLOSED_MARKER) {
            sawClosedMarker = true;
          } else {
            logWork.latch.countDown();
            logWork.cleanUp();
          }
        }
        if (sawClosedMarker) {
          break;
        }

      }
    }
  }

  static class LogWork {
    CountDownLatch latch;
    volatile Exception exception;
    DataOutputStream out;
    ByteArrayOutputStream dataOut;
    byte[] data = null;
    boolean compress;
    
    public LogWork(CountDownLatch latch, boolean compress) {
      this.latch = latch;
      if (latch != null) {
        this.dataOut = new ByteArrayOutputStream();
        this.out = new DataOutputStream(this.dataOut);
      }
      this.compress = compress;
    }
    
    public DataOutputStream getDataOutputStream() {
        return out;
    }
    public byte[] getData() {
      return data;
    }
    public boolean hasData() {
      if (data == null) {
        return false;
      } else {
        return true;
      }
    }
    public void cleanUp() {
      data = null;
    }
    public void ready() {
      try {
        if (out != null && dataOut != null) {
         
          out.close();
          dataOut.close();
          if (compress) {
            byte[] predata = dataOut.toByteArray();
            
            byte[] compressed = Snappy.compress(predata);
            //write the length of the compressed array using the DataOutputStream writeInt() method
            int blockLength = compressed.length;
            data = new byte[blockLength + 4];
            data[0] = (byte) ((blockLength >>> 24) & 0xFF);
            data[1] = (byte) ((blockLength >>> 16) & 0xFF);
            data[2] = (byte) ((blockLength >>>  8) & 0xFF);
            data[3] = (byte) ((blockLength >>>  0) & 0xFF);
            System.arraycopy(compressed, 0, data, 4, blockLength);
            
            predata = null;
            compressed = null;
          } else {
            data = dataOut.toByteArray();
          }
          out = null;
          dataOut = null;
        }
      } catch (IOException e) {
        log.error("Error cleaning up work obj", e);
      }
    }
  }

  public static class LoggerOperation {
    private final LogWork work;

    public LoggerOperation(LogWork work) {
      this.work = work;
    }

    public void await() throws IOException {
      try {
        work.latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (work.exception != null) {
        if (work.exception instanceof IOException)
          throw (IOException) work.exception;
        else if (work.exception instanceof RuntimeException)
          throw (RuntimeException) work.exception;
        else
          throw new RuntimeException(work.exception);
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    // filename is unique
    if (obj == null)
      return false;
    if (obj instanceof DfsLogger)
      return getFileName().equals(((DfsLogger) obj).getFileName());
    return false;
  }

  @Override
  public int hashCode() {
    // filename is unique
    return getFileName().hashCode();
  }

  private final ServerResources conf;
  private FSDataOutputStream logFile;
  private DataOutputStream encryptingLogFile = null;
  private Method sync;
  private String logPath;
  
  public DfsLogger(ServerResources conf) throws IOException {
    this.conf = conf;
  }

  public DfsLogger(ServerResources conf, String filename) throws IOException {
    this.conf = conf;
    this.logPath = filename;
  }


  public static DFSLoggerInputStreams readHeaderAndReturnStream(VolumeManager fs, Path path, AccumuloConfiguration conf) throws IOException {
    FSDataInputStream input = fs.open(path);
    DataInputStream decryptingInput = null, tmpInput;
    
    byte[] compressed = DfsLogger.LOG_COMPRESSION_HEADER.getBytes();
    byte[] compressedBuffer = new byte[compressed.length];
    byte[] magic = DfsLogger.LOG_FILE_HEADER_V3.getBytes();
    byte[] magicBuffer = new byte[magic.length];
    input.readFully(magicBuffer);
    if (Arrays.equals(magicBuffer, magic)) {
      // additional parameters it needs from the underlying stream.
      String cryptoModuleClassname = input.readUTF();
      CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(cryptoModuleClassname);

      // Create the parameters and set the input stream into those parameters
      CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
      params.setEncryptedInputStream(input);

      // Create the plaintext input stream from the encrypted one
      params = cryptoModule.getDecryptingInputStream(params);

      if (params.getPlaintextInputStream() instanceof DataInputStream) {
        tmpInput = (DataInputStream) params.getPlaintextInputStream();
      } else {
        tmpInput = new DataInputStream(params.getPlaintextInputStream());
      }
      
      //If Log file has been compressed wrap the decryptingInputStream in a DecompressingInputStream
      input.readFully(compressedBuffer);
      if (!Arrays.equals(compressed, compressedBuffer)) {
        input.seek(input.getPos() - compressed.length);
        decryptingInput = tmpInput;
      } else {
        decryptingInput = new DecompressingInputStream(tmpInput);
      }
    } else {
      input.seek(0);
      byte[] magicV2 = DfsLogger.LOG_FILE_HEADER_V2.getBytes();
      byte[] magicBufferV2 = new byte[magic.length];
      input.readFully(magicBufferV2);

      if (Arrays.equals(magicBufferV2, magicV2)) {
        // Log files from 1.5 dump their options in raw to the logger files. Since we don't know the class
        // that needs to read those files, we can make a couple of basic assumptions. Either it's going to be
        // the NullCryptoModule (no crypto) or the DefaultCryptoModule.

        // If it's null, we won't have any parameters whatsoever. First, let's attempt to read
        // parameters
        Map<String,String> opts = new HashMap<String,String>();
        int count = input.readInt();
        for (int i = 0; i < count; i++) {
          String key = input.readUTF();
          String value = input.readUTF();
          opts.put(key, value);
        }

        if (opts.size() == 0) {
          // NullCryptoModule, we're done
          decryptingInput = input;
        } else {

          // The DefaultCryptoModule will want to read the parameters from the underlying file, so we will put the file back to that spot.
          org.apache.accumulo.core.security.crypto.CryptoModule cryptoModule = org.apache.accumulo.core.security.crypto.CryptoModuleFactory
              .getCryptoModule(DefaultCryptoModule.class.getName());

          CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);

          input.seek(0);
          input.readFully(magicBuffer);
          params.setEncryptedInputStream(input);

          params = cryptoModule.getDecryptingInputStream(params);
          if (params.getPlaintextInputStream() instanceof DataInputStream) {
            decryptingInput = (DataInputStream) params.getPlaintextInputStream();
          } else {
            decryptingInput = new DataInputStream(params.getPlaintextInputStream());
          }
        }

      } else {

        input.seek(0);
        decryptingInput = input;
      }

    }
    return new DFSLoggerInputStreams(input, decryptingInput);
  }

  public synchronized void open(String address) throws IOException {
    String filename = UUID.randomUUID().toString();
    String logger = Joiner.on("+").join(address.split(":"));

    log.debug("DfsLogger.open() begin");
    VolumeManager fs = conf.getFileSystem();

    logPath = fs.choose(ServerConstants.getWalDirs()) + "/" + logger + "/" + filename;
    try {
      short replication = (short) conf.getConfiguration().getCount(Property.TSERV_WAL_REPLICATION);
      if (replication == 0)
        replication = fs.getDefaultReplication(new Path(logPath));
      long blockSize = conf.getConfiguration().getMemoryInBytes(Property.TSERV_WAL_BLOCKSIZE);
      if (blockSize == 0)
        blockSize = (long) (conf.getConfiguration().getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE) * 1.1);
      if (conf.getConfiguration().getBoolean(Property.TSERV_WAL_SYNC))
        logFile = fs.createSyncable(new Path(logPath), 0, replication, blockSize);
      else
        logFile = fs.create(new Path(logPath), true, 0, replication, blockSize);
      
      useCompression = conf.getConfiguration().getBoolean(Property.TSERV_WAL_COMPRESSION);
      
      try {
        NoSuchMethodException e = null;
        try {
          // sync: send data to datanodes
          sync = logFile.getClass().getMethod("sync");
        } catch (NoSuchMethodException ex) {
          e = ex;
        }
        try {
          // hsync: send data to datanodes and sync the data to disk
          sync = logFile.getClass().getMethod("hsync");
          e = null;
        } catch (NoSuchMethodException ex) {}
        if (e != null)
          throw new RuntimeException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      // Initialize the crypto operations.
      org.apache.accumulo.core.security.crypto.CryptoModule cryptoModule = org.apache.accumulo.core.security.crypto.CryptoModuleFactory.getCryptoModule(conf
          .getConfiguration().get(Property.CRYPTO_MODULE_CLASS));
      
      // Initialize the log file with a header and the crypto params used to set up this log file.
      logFile.write(LOG_FILE_HEADER_V3.getBytes(StandardCharsets.UTF_8));
      log.info("File opened: " + filename);
      CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf.getConfiguration());

      NoFlushOutputStream nfos = new NoFlushOutputStream(logFile);
      params.setPlaintextOutputStream(nfos);

      // In order to bootstrap the reading of this file later, we have to record the CryptoModule that was used to encipher it here,
      // so that that crypto module can re-read its own parameters.

      logFile.writeUTF(conf.getConfiguration().get(Property.CRYPTO_MODULE_CLASS));
      
      params = cryptoModule.getEncryptingOutputStream(params);
      OutputStream encipheringOutputStream = params.getEncryptedOutputStream();
      
      //If the Log entries are going to be compressed record that this file is using compression
      if (useCompression) {
        logFile.write(LOG_COMPRESSION_HEADER.getBytes(StandardCharsets.UTF_8));
      }
      
      // If the module just kicks back our original stream, then just use it, don't wrap it in
      // another data OutputStream.
      if (encipheringOutputStream == nfos) {
        log.debug("No enciphering, using raw output stream");
        encryptingLogFile = nfos;
      } else {
        log.debug("Enciphering found, wrapping in DataOutputStream");
        encryptingLogFile = new DataOutputStream(encipheringOutputStream);
      }
      
      DfsLogger.LogWork work = new DfsLogger.LogWork(new CountDownLatch(1), useCompression);
      
      LogFileKey key = new LogFileKey();
      key.event = OPEN;
      key.tserverSession = filename;
      key.filename = filename;
      write(work, key, EMPTY);
      work.ready();
      if (work.hasData()) {
        encryptingLogFile.write(work.getData());
        encryptingLogFile.flush();
      }
      sync.invoke(logFile);
      work.cleanUp();
      log.debug("Got new write-ahead log: " + this);
    } catch (Exception ex) {
      if (logFile != null)
        logFile.close();
      logFile = null;
      encryptingLogFile = null;
      throw new IOException(ex);
    }

    flushingThread = new Daemon(new LogSyncingTask());
    flushingThread.setName("Accumulo WALog thread " + toString());
    flushingThread.start();
  }

  @Override
  public String toString() {
    String fileName = getFileName();
    if (fileName.contains(":"))
      return getLogger() + "/" + getFileName();
    return fileName;
  }

  public String getFileName() {
    return logPath.toString();
  }

  public void close() throws IOException {
   
    synchronized (closeLock) {
      if (closed)
        return;
      // after closed is set to true, nothing else should be added to the queue
      // CLOSED_MARKER should be the last thing on the queue, therefore when the
      // background thread sees the marker and exits there should be nothing else
      // to process... so nothing should be left waiting for the background
      // thread to do work
      closed = true;
    }
    
    workQueue.add(CLOSED_MARKER);

    try {
      //when the flushing thread terminates then finish closing the Log file
      flushingThread.join();
    } catch (InterruptedException e) {
      log.info("Interrupted");
    }

    if (encryptingLogFile != null)
      try {
        logFile.close();
        logFile = null;
      } catch (IOException ex) {
        log.error(ex);
        throw new LogClosedException();
      }

  }

  public void defineTablet(int seq, int tid, KeyExtent tablet) throws IOException {
    DfsLogger.LogWork work = new DfsLogger.LogWork(new CountDownLatch(1), useCompression);
    
    // write this log to the METADATA table
    final LogFileKey key = new LogFileKey();
    key.event = DEFINE_TABLET;
    key.seq = seq;
    key.tid = tid;
    key.tablet = tablet;
    try {
      write(work, key, EMPTY);
      work.ready();
      if (work.hasData()) {
        encryptingLogFile.write(work.getData());
        encryptingLogFile.flush();
      }
    } catch (IllegalArgumentException e) {
      log.error("Signature of sync method changed. Accumulo is likely incompatible with this version of Hadoop.");
      throw new RuntimeException(e);
    }
    synchronized (closeLock) {
      if (closed) {
        throw new LogClosedException();
      }
      workQueue.add(work);
    }
  }
  
  private void write(DfsLogger.LogWork work, LogFileKey key, LogFileValue value) throws IOException {
    
    key.write(work.getDataOutputStream());
    value.write(work.getDataOutputStream());
    
  }

  public LoggerOperation log(int seq, int tid, Mutation mutation) throws IOException {
    return logManyTablets(Collections.singletonList(new TabletMutations(tid, seq, Collections.singletonList(mutation))));
  }

  private LoggerOperation logFileData(List<Pair<LogFileKey,LogFileValue>> keys) throws IOException {
    DfsLogger.LogWork work = new DfsLogger.LogWork(new CountDownLatch(1), useCompression);

    try {
      for (Pair<LogFileKey,LogFileValue> pair : keys) {
        write(work, pair.getFirst(), pair.getSecond());
      }
      work.ready();
      if (work.hasData()) {
        encryptingLogFile.write(work.getData());
        encryptingLogFile.flush();
      }
    } catch (Exception e) {
      log.error(e, e);
      work.exception = e;
    }
    synchronized(closeLock) {
      if (closed) {
        throw new LogClosedException();
      } else {
        workQueue.add(work);
      }
    }
    
    return new LoggerOperation(work);
  }

  public LoggerOperation logManyTablets(List<TabletMutations> mutations) throws IOException {
    List<Pair<LogFileKey,LogFileValue>> data = new ArrayList<Pair<LogFileKey,LogFileValue>>();
    for (TabletMutations tabletMutations : mutations) {
      LogFileKey key = new LogFileKey();
      key.event = MANY_MUTATIONS;
      key.seq = tabletMutations.getSeq();
      key.tid = tabletMutations.getTid();
      LogFileValue value = new LogFileValue();
      value.mutations = tabletMutations.getMutations();
      data.add(new Pair<LogFileKey,LogFileValue>(key, value));
    }
    return logFileData(data);
  }

  public LoggerOperation minorCompactionFinished(int seq, int tid, String fqfn) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_FINISH;
    key.seq = seq;
    key.tid = tid;
    return logFileData(Collections.singletonList(new Pair<LogFileKey,LogFileValue>(key, EMPTY)));
  }

  public LoggerOperation minorCompactionStarted(int seq, int tid, String fqfn) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_START;
    key.seq = seq;
    key.tid = tid;
    key.filename = fqfn;
    return logFileData(Collections.singletonList(new Pair<LogFileKey,LogFileValue>(key, EMPTY)));
  }

  public String getLogger() {
    String parts[] = logPath.split("/");
    return Joiner.on(":").join(parts[parts.length - 2].split("[+]"));
  }

}
