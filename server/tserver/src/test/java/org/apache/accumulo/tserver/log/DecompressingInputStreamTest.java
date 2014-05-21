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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Test;
import org.xerial.snappy.Snappy;

/**
 * 
 */
public class DecompressingInputStreamTest {
  private static byte hasValues = (byte) 1;
  private static byte VAL1 = ((byte) (0x80 | hasValues));
  private static byte VAL2 = ((byte) (0x80 | 0x40));
  private static byte VAL3 = 0x05;
  private static byte VAL4 = 0x03;
  static final String LOG_FILE_HEADER_V3 = "--- Log File Header (v3) ---";

  @Test
  public void testRead() throws IOException {
    byte[] b = buildArray(1);
    ByteArrayInputStream input = new ByteArrayInputStream(b);
    DecompressingInputStream in = new DecompressingInputStream(input);

    assertTrue(in.read() == (VAL1 & 0xff));
    assertTrue(in.read() == (VAL2 & 0xff));
    assertTrue(in.read() == (VAL3));
    assertTrue(in.read() == (VAL4));
    assertTrue(in.read() == -1);
    in.close();
  }

  @Test
  public void testReadBytes() throws IOException {
    byte[] b = buildArray(1);
    ByteArrayInputStream input = new ByteArrayInputStream(b);
    DecompressingInputStream in = new DecompressingInputStream(input);
    byte data[] = new byte[4];

    int num = in.read(data);
    assertTrue(num == 4);
    assertTrue(data[0] == VAL1);
    assertTrue(data[1] == VAL2);
    assertTrue(data[2] == VAL3);
    assertTrue(data[3] == VAL4);

    num = in.read(data);
    assertTrue(num == -1);
    in.close();
  }

  @Test
  public void testReadMoreBytes() throws IOException {
    byte[] b = buildArray(2);
    ByteArrayInputStream input = new ByteArrayInputStream(b);
    DecompressingInputStream in = new DecompressingInputStream(input);
    byte data[] = new byte[3];
    int num;
    assertTrue(in.read() == (VAL1 & 0xff));
    num = in.read(data, 1, 2);
    assertTrue(num == 2);
    assertTrue(data[0] == 0);
    assertTrue(data[1] == (VAL2));
    assertTrue(data[2] == VAL3);

    data = new byte[5];

    assertTrue(in.read() == 3);
    num = in.read(data);
    assertTrue(num == 4);
    assertTrue(data[0] == (VAL1));
    assertTrue(data[3] == VAL4);
    assertTrue(data[4] == 0);
    in.close();
  }

  @Test
  public void testIncompleteFile() throws IOException {
    byte[] b = buildBadArray(2);
    ByteArrayInputStream input = new ByteArrayInputStream(b);
    DecompressingInputStream in = new DecompressingInputStream(input);
    byte data[] = new byte[8];
    int num;

    num = in.read(data);
    assertTrue(num == 4);
    assertTrue(data[0] == VAL1);
    assertTrue(data[1] == VAL2);
    assertTrue(data[2] == VAL3);
    assertTrue(data[3] == VAL4);
    assertTrue(data[4] == 0);
    assertTrue(data[5] == 0);
    assertTrue(data[6] == 0);
    assertTrue(data[7] == 0);

    in.close();
  }

  private static byte[] buildArray(int numBlocks) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream tempOutPut = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(output);
    for (int i = 0; i < numBlocks; i++) {

      tempOutPut.write(VAL1);
      tempOutPut.write(VAL2);
      tempOutPut.write(VAL3);
      tempOutPut.write(VAL4);

      byte[] piece = Snappy.compress(tempOutPut.toByteArray());

      data.writeInt(piece.length);
      data.write(piece);
      tempOutPut = new ByteArrayOutputStream();
    }
    data.flush();
    data.close();
    return output.toByteArray();
  }

  private static byte[] buildBadArray(int numBlocks) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream tempOutPut = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(output);
    for (int i = 1; i < numBlocks; i++) {

      tempOutPut.write(VAL1);
      tempOutPut.write(VAL2);
      tempOutPut.write(VAL3);
      tempOutPut.write(VAL4);

      byte[] piece = Snappy.compress(tempOutPut.toByteArray());

      data.writeInt(piece.length);
      data.write(piece);
      tempOutPut = new ByteArrayOutputStream();
    }
    tempOutPut.write(VAL1);
    tempOutPut.write(VAL2);
    tempOutPut.write(VAL3);
    tempOutPut.write(VAL4);

    byte[] piece = Snappy.compress(tempOutPut.toByteArray());

    data.writeInt(piece.length + 8);
    data.write(piece);

    data.flush();
    data.close();
    return output.toByteArray();
  }
}
