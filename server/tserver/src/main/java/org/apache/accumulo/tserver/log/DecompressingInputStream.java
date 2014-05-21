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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.xerial.snappy.Snappy;

public class DecompressingInputStream extends DataInputStream {

  /**
   * @param in
   */
  public DecompressingInputStream(InputStream in) {
    super(new DecompressingStream(in));
  }

}

/*
 * Maintains an internal buffer and position counter. As the 
 * counter reaches the end of the buffer the next chunk of compressed
 * bytes is read from the underlying input stream.
 */
class DecompressingStream extends FilterInputStream {
  byte[] buffer;
  int bufferPos;

  public DecompressingStream(InputStream in) {
    super(in);
    buffer = new byte[0];
    bufferPos = 0;
  }

  public void checkBuffer() throws IOException {
    if (bufferPos == buffer.length) {

      int chunk = readInt(in);
      
      byte[] compressed = new byte[chunk];

      int bytesRead = readCompressed(compressed, in);
      
      //If the entire chunk could not be read it cannot be
      // uncompressed and is presumed to be unrecoverable
      if (bytesRead < chunk) {
        throw new EOFException();
      }
      
      buffer = Snappy.uncompress(compressed);
      
      bufferPos = 0;
    }
  }

  private int readInt(InputStream input) throws IOException {
    int ch1 = input.read();

    int ch2 = input.read();

    int ch3 = input.read();

    int ch4 = input.read();

    if ((ch1 | ch2 | ch3 | ch4) < 0) {
      throw new EOFException();
    }
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  @Override
  public int read() throws IOException {
    try {
      checkBuffer();
    } catch (EOFException e) {
      return -1;
    }
    int num = buffer[bufferPos] & 0xff;
    bufferPos++;
    return num;
  }

  private int readCompressed(byte[] b, InputStream input) throws IOException {
    return readCompressed(b, 0, b.length, input);
  }

  private int readCompressed(byte b[], int off, int len, InputStream input) throws IOException {
    int read = 0;
    int total = 0;
    
    // continue to read while the total bytes read is less then the expected len
    do {
      read = input.read(b, (off + total), (len - total));
      if (read < 0) {
        //unexpected end of file
        break;
      } else {
        total += read;
      }
    } while (total < len);
    
    return total;

  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    try {
      checkBuffer();
    } catch (IOException e) {
      return -1; // end of file
    }
    int bytesCopied = 0, bytesToRead = 0, destPos = off;
    boolean endOfFile = false;
    while (bytesCopied < len && !endOfFile) {
      try {
        checkBuffer();
        bytesToRead = buffer.length - bufferPos;
        bytesToRead = bytesToRead < len ? bytesToRead : len;
        destPos += bytesCopied;
        System.arraycopy(buffer, bufferPos, b, destPos, bytesToRead);
        bufferPos += bytesToRead;
        bytesCopied += bytesToRead;
      } catch (IOException e) {
        endOfFile = true; // end of file
      }
    }
    return bytesCopied;

  }

}
