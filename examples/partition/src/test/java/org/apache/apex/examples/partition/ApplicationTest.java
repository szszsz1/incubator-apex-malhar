/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.partition;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(5000); // runs for 5 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}

