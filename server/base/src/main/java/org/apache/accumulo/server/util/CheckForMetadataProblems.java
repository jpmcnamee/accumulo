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
package org.apache.accumulo.server.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class CheckForMetadataProblems {
  private static boolean sawProblems = false;
  
  public static void checkTable(String tablename, TreeSet<KeyExtent> tablets, Opts opts) throws AccumuloSecurityException {
    // sanity check of metadata table entries
    // make sure tablets has no holes, and that it starts and ends w/ null
    
    if (tablets.size() == 0) {
      System.out.println("No entries found in metadata table for table " + tablename);
      sawProblems = true;
      return;
    }
    
    if (tablets.first().getPrevEndRow() != null) {
      System.out.println("First entry for table " + tablename + "- " + tablets.first() + " - has non null prev end row");
      sawProblems = true;
      return;
    }
    
    if (tablets.last().getEndRow() != null) {
      System.out.println("Last entry for table " + tablename + "- " + tablets.last() + " - has non null end row");
      sawProblems = true;
      return;
    }
    
    Iterator<KeyExtent> tabIter = tablets.iterator();
    Text lastEndRow = tabIter.next().getEndRow();
    boolean everythingLooksGood = true;
    while (tabIter.hasNext()) {
      KeyExtent tabke = tabIter.next();
      boolean broke = false;
      if (tabke.getPrevEndRow() == null) {
        System.out.println("Table " + tablename + " has null prev end row in middle of table " + tabke);
        broke = true;
      } else if (!tabke.getPrevEndRow().equals(lastEndRow)) {
        System.out.println("Table " + tablename + " has a hole " + tabke.getPrevEndRow() + " != " + lastEndRow);
        broke = true;
      }
      if (broke) {
        everythingLooksGood = false;
      }
      
      lastEndRow = tabke.getEndRow();
    }
    if (everythingLooksGood)
      System.out.println("All is well for table " + tablename);
    else
      sawProblems = true;
  }
  
  public static void checkMetadataAndRootTableEntries(String tableNameToCheck, Opts opts, VolumeManager fs) throws Exception {
	System.out.println("Checking table: " + tableNameToCheck);
    Map<String,TreeSet<KeyExtent>> tables = new HashMap<String,TreeSet<KeyExtent>>();
    
    Scanner scanner;
    
    if (opts.offline) {
      scanner = new OfflineMetadataScanner(ServerConfiguration.getSystemConfiguration(opts.getInstance()), fs);
    } else {
      scanner = opts.getConnector().createScanner(tableNameToCheck, Authorizations.EMPTY);
    }
    
    scanner.setRange(MetadataSchema.TabletsSection.getRange());
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
    
    Text colf = new Text();
    Text colq = new Text();
    boolean justLoc = false;
    
    int count = 0;
    
    for (Entry<Key,Value> entry : scanner) {
      colf = entry.getKey().getColumnFamily(colf);
      colq = entry.getKey().getColumnQualifier(colq);
      
      count++;
      
      String tableName = (new KeyExtent(entry.getKey().getRow(), (Text) null)).getTableId().toString();
      
      TreeSet<KeyExtent> tablets = tables.get(tableName);
      if (tablets == null) {
        Set<Entry<String,TreeSet<KeyExtent>>> es = tables.entrySet();
        
        for (Entry<String,TreeSet<KeyExtent>> entry2 : es) {
          checkTable(entry2.getKey(), entry2.getValue(), opts);
        }
        
        tables.clear();
        
        tablets = new TreeSet<KeyExtent>();
        tables.put(tableName, tablets);
      }
      
      if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.equals(colf, colq)) {
        KeyExtent tabletKe = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        tablets.add(tabletKe);
        justLoc = false;
      } else if (colf.equals(TabletsSection.CurrentLocationColumnFamily.NAME)) {
        if (justLoc) {
          System.out.println("Problem at key " + entry.getKey());
          sawProblems = true;
        }
        justLoc = true;
      }
    }
    
    if (count == 0) {
      System.err.println("ERROR : " + tableNameToCheck + " table is empty");
      sawProblems = true;
    }
    
    Set<Entry<String,TreeSet<KeyExtent>>> es = tables.entrySet();
    
    for (Entry<String,TreeSet<KeyExtent>> entry : es) {
      checkTable(entry.getKey(), entry.getValue(), opts);
    }
    
    if (!sawProblems) {
    	System.out.println("No problems found");
    }
    // end METADATA table sanity check
  }
  
  static class Opts extends ClientOpts {
    @Parameter(names = "--offline", description = "perform the check on the files directly")
    boolean offline = false;
  }
  
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(CheckForMetadataProblems.class.getName(), args);
    Opts dummyOpts = new Opts();
    dummyOpts.auths=opts.auths;
    dummyOpts.password=opts.password;
    
    VolumeManager fs = VolumeManagerImpl.get();
    
    checkMetadataAndRootTableEntries(RootTable.NAME, dummyOpts, fs);
    checkMetadataAndRootTableEntries(MetadataTable.NAME, opts, fs);
    dummyOpts.stopTracing();
    opts.stopTracing();
    if (sawProblems)
      throw new RuntimeException();
  }
  
}
