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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.tserver.TabletMutations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * 
 */
public class LogTester {
  
  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    String path = null;
    int writers = 1;
    int flushInterval = 10000;
    int numFlushes = 1;
    
    if (args.length < 1) {
      System.out.println("invalid args usage: path numWriters");
      return;
    } else {
      path = args[0];
    }
    if (args.length >= 2) {
      writers = Integer.parseInt(args[1]);
    }
    if (args.length >= 3) {
      flushInterval = Integer.parseInt(args[2]);
    }
    if (args.length >= 4) {
      numFlushes = Integer.parseInt(args[3]);
    }
    
    final VolumeManager fs = VolumeManagerImpl.get();
    Instance instance = HdfsZooInstance.getInstance();
    final ServerConfiguration conf = new ServerConfiguration(instance);

    DfsLogger log = new DfsLogger(new DfsLogger.ServerResources() {

      @Override
      public VolumeManager getFileSystem() {
        return fs;
      }

      @Override
      public Set<TServerInstance> getCurrentTServers() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public AccumuloConfiguration getConfiguration() {
        return conf.getConfiguration();
      }
    });
    log.open(path);
    System.out.println("Using " + writers + " writers, writing to: " + log.getFileName());
    long stop, start = System.currentTimeMillis();
    
    Thread[] threadPool = new Thread[writers];
    for (int i = 0; i < writers; i++) {
      threadPool[i] = new Thread(new writer(log, flushInterval, numFlushes, i));
      threadPool[i].start();
    }
    for (int y = 0; y < writers; y++) {
      threadPool[y].join();
    }
    stop = System.currentTimeMillis();
    log.close();
    
    System.out.println("done");
    System.out.println("total run time: " + (stop - start));
  }

}

class writer implements Runnable {
  private static int min = 1;
  private static int max = 10000;
  private static int maxColF = 10000;
  private static int maxColQ = 10000;
  private static final byte[] EMPTY_BYTES = new byte[0];
  
  DfsLogger dlog;
  int id, flushInterval, numFlushes;
  String path;
  private static List<ColumnVisibility> visibilities;

  private static void initVisibilities() throws Exception {

    visibilities = Collections.singletonList(new ColumnVisibility());

  }

  public writer(DfsLogger a, int flushInt, int num, int id) {
    dlog = a;
    this.id = id;
    this.flushInterval = flushInt;
    this.numFlushes = num;
  }

  @Override
  public void run() {
    byte[] ingestInstanceId = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    long start, stop;
    Random r = new Random();
    try {
      initVisibilities();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    long count = 0;
    long prevRows[] = new long[flushInterval];
    long firstRows[] = new long[flushInterval];
    int firstColFams[] = new int[flushInterval];
    int firstColQuals[] = new int[flushInterval];
    ColumnVisibility cv = getVisibility(r);
    
    for (int i = 0; i < numFlushes; i++) {
      ArrayList<Mutation> mutations = new ArrayList<Mutation>();
      for (int index = 0; index < flushInterval; index++) {
        long rowLong = genLong(min, max, r);
        prevRows[index] = rowLong;
        firstRows[index] = rowLong;
        
        int cf = r.nextInt(maxColF);
        int cq = r.nextInt(maxColQ);
        
        firstColFams[index] = cf;
        firstColQuals[index] = cq;
        
        Mutation m = genMutation(rowLong, cf, cq, cv, ingestInstanceId, count, null, r, false);
        count++;
        mutations.add(m);
       
        
      }
      List<TabletMutations> info = Collections.singletonList(new TabletMutations(1, 1, mutations));
      
      try {
        //start = System.currentTimeMillis();
        DfsLogger.LoggerOperation l = dlog.logManyTablets(info);
        //stop = System.currentTimeMillis();
        //System.out.println("Producer " + id + " took " + (stop - start) + " ms to produce");
        start = System.currentTimeMillis();
        l.await();
        stop = System.currentTimeMillis();
        System.out.println("Producer " + id + " waited " + (stop - start) + " for consumer");
        
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  public static final long genLong(long min, long max, Random r) {
    return ((r.nextLong() & 0x7fffffffffffffffl) % (max - min)) + min;
  }
  private static ColumnVisibility getVisibility(Random rand) {
    return visibilities.get(rand.nextInt(visibilities.size()));
  }
  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, ColumnVisibility cv, byte[] ingestInstanceId, long count, byte[] prevRow, Random r,
      boolean checksum) {
    // Adler32 is supposed to be faster, but according to wikipedia is not good for small data.... so used CRC32 instead
    CRC32 cksum = null;
    
    byte[] rowString = genRow(rowLong);
    
    byte[] cfString = FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
    byte[] cqString = FastFormat.toZeroPaddedString(cqInt, 4, 16, EMPTY_BYTES);
    
    if (checksum) {
      cksum = new CRC32();
      cksum.update(rowString);
      cksum.update(cfString);
      cksum.update(cqString);
      cksum.update(cv.getExpression());
    }
    
    Mutation m = new Mutation(new Text(rowString));
    
    m.put(new Text(cfString), new Text(cqString), cv, createValue(ingestInstanceId, count, prevRow, cksum));
    return m;
  }
  static final byte[] genRow(long rowLong) {
    return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
  }
  private static Value createValue(byte[] ingestInstanceId, long count, byte[] prevRow, Checksum cksum) {
    int dataLen = ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + 3;
    if (cksum != null)
      dataLen += 8;
    byte val[] = new byte[dataLen];
    System.arraycopy(ingestInstanceId, 0, val, 0, ingestInstanceId.length);
    int index = ingestInstanceId.length;
    val[index++] = ':';
    int added = FastFormat.toZeroPaddedString(val, index, count, 16, 16, EMPTY_BYTES);
    if (added != 16)
      throw new RuntimeException(" " + added);
    index += 16;
    val[index++] = ':';
    if (prevRow != null) {
      System.arraycopy(prevRow, 0, val, index, prevRow.length);
      index += prevRow.length;
    }
    
    val[index++] = ':';
    
    if (cksum != null) {
      cksum.update(val, 0, index);
      cksum.getValue();
      FastFormat.toZeroPaddedString(val, index, cksum.getValue(), 8, 16, EMPTY_BYTES);
    }
    
    // System.out.println("val "+new String(val));
    
    return new Value(val);
  }
}
