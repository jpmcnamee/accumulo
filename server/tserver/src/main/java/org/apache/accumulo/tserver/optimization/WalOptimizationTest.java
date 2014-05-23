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
package org.apache.accumulo.tserver.optimization;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.xerial.snappy.Snappy;
public class WalOptimizationTest {
	
	private static List<byte[]> wordList;
	
	public static void buildListFromFile(String file) {
		wordList = new ArrayList<byte[]>();
		try{
		    BufferedReader reader = new BufferedReader(new FileReader(file));
		    String word = reader.readLine();
		    
		    while(word != null) {
		    	wordList.add(word.getBytes());
		        word = reader.readLine();
		    }
		    reader.close();
		} catch (Exception e) {
		    e.printStackTrace();
		}
		Collections.shuffle(wordList);
		
	}
	public static byte[] randomWord(int length) {
		int num = (int) (Math.random() * length);
		return wordList.get(num);
	}
	public static void main(String[] args) throws IOException, SecurityException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
		int entropy = -1;
		int rows = 100;
		int columns = 1;
		String loc, words = "/usr/share/dict/words";
		
		try {
			if (args.length == 0) {
				System.out.println("Usage: WalOptimization /path/to/wal rows cols entropy /path/to/wordlist");
				return;
			}
			if (args.length >= 1) {
				loc = args[0];
				System.out.println("Writing WAL to: " + loc);
			} else {
				System.out.println("please specifiy file location");
				return;
			}
			
			if (args.length >= 2) {
				rows = Integer.parseInt(args[1]);
			}
			if (args.length >= 3) {
				columns = Integer.parseInt(args[2]);
			}
			if (args.length >= 4) {
				entropy = Integer.parseInt(args[3]);
			}
			if (args.length >= 5) {
				words = args[4];
			}
		}catch (Exception e) {
			System.out.println("An error occured!");
			System.out.println("Usage: WalOptimization /path/to/wal rows cols entropy /path/to/wordlist");
			return;
		}
		
		FileSystem fs = FileSystem.get(new Configuration());
		//FileOutputStream fout = new FileOutputStream(loc);
		//BufferedOutputStream buff = new BufferedOutputStream(fout);
		
		//FSDataOutputStream out = new FSDataOutputStream(gzout);
		FSDataOutputStream out = fs.create(new Path(loc), true, 20000);
		//ByteArrayOutputStream bytes = new ByteArrayOutputStream(30000);
		//GZIPOutputStream gzout = new GZIPOutputStream(bytes, 30000);
		//FSDataOutputStream out = fs.create(new Path(loc));
		
		ColumnVisibility cv = new ColumnVisibility();
		System.out.println("Building word list");
		buildListFromFile(words);
		System.out.println("Word list built");
		int MUTATIONS =rows * columns;
		
		int wordListLength = wordList.size();
		System.out.println("Num words " + wordListLength);
		if (entropy < 0 || entropy > wordListLength) {
			entropy = wordListLength;
		}
		ByteArrayOutputStream chunk = new ByteArrayOutputStream(2000000);
		long stopTime, startTime = System.currentTimeMillis();
		long tempStop, tempStart;
		double elapsed, writingElapsed = 0, constructorElapsed = 0;
		
		for (int i = 0; i < rows; i++) {
			byte[] row = new String("row" + i).getBytes();
			
			//byte[] row = "row".getBytes();
			tempStart = System.currentTimeMillis();
			Mutation m = new Mutation(row);
			//CompressedMutation m = new CompressedMutation(row);
			//m.setCompression(compressionContext);
			tempStop = System.currentTimeMillis();
			constructorElapsed += (tempStop - tempStart) / 1000.0;
			for (int j = 0; j < columns; j++) {
				byte[] family = randomWord(entropy);
				//byte[] family = "family".getBytes();
				byte[] qualifier = randomWord(entropy);
				//byte[] qualifier = "qualifier".getBytes();
				byte[] value = randomWord(wordListLength);
				//byte[] value = "value".getBytes();
				
				tempStart = System.currentTimeMillis();
				m.put(family, qualifier, cv, value);
				tempStop = System.currentTimeMillis();
				constructorElapsed += (tempStop - tempStart) / 1000.0;
				//System.out.println("Constructor time: " + elapsed);
			}
			tempStart = System.currentTimeMillis();
			m.write2(chunk);
			tempStop = System.currentTimeMillis();
			writingElapsed += (tempStop - tempStart) / 1000.0;
			//System.out.println(out.size());
			//System.out.println("write time: " + elapsed);
			if (i % 100000 == 0) {
				tempStart = System.currentTimeMillis();
				byte[] sofar = chunk.toByteArray();
				//System.out.println(sofar.length);
				chunk.close();
				byte[] compressed = Snappy.compress(sofar);
				out.write(compressed);
				//out.flush();
				out.hsync();
				chunk = new ByteArrayOutputStream(2000000);
				tempStop = System.currentTimeMillis();
				writingElapsed += (tempStop - tempStart) / 1000.0;
			}
		}
		//byte[] sofar = chunk.toByteArray();
		//chunk.close();
		//byte[] compressed = Snappy.compress(sofar);
		tempStart = System.currentTimeMillis();
		chunk.close();
		byte[] compressed = Snappy.compress(chunk.toByteArray());
		out.write(compressed);
		out.hsync();
		//out.flush();
		tempStop = System.currentTimeMillis();
		writingElapsed += (tempStop - tempStart) / 1000.0;
		//gzout.finish();
		//gzout.close();
		//bytes.close();
		//out.write(bytes.toByteArray());
		out.close();
		stopTime = System.currentTimeMillis();
	    
	    fs.close();
	    int totalValues = MUTATIONS;
	    elapsed = (stopTime - startTime) / 1000.0;
	    
	    System.out.printf("%,12d records written | %,8d records/sec |  %6.3f secs   %n", totalValues,
	        (int) (totalValues / elapsed), elapsed);
	    System.out.println("time spent writing: " + writingElapsed);
	    System.out.println("time spent in constructor: " + constructorElapsed);
	
		

	}

}
