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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.start.TestMain;
import org.junit.Test;

public class StartIT extends ConfigurableMacIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @Test
  public void test() throws Exception {
    assertTrue(exec(TestMain.class, "exception").waitFor() != 0);
    assertEquals(0, exec(TestMain.class, "success").waitFor());
    assertTrue(exec(TestMain.class).waitFor() != 0);
  }

}
