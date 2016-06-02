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
package com.datatorrent.lib.stream;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Module;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.CountTestSink;
import com.datatorrent.lib.testbench.RandomWordGenerator;

public class MultipleStreamMergerTest {
  private static Logger LOG = LoggerFactory.getLogger(MultipleStreamMergerTest.class);

  @Test
  public void mergeTwoStreams() {
    throw new NotImplementedException();
  }

  @Test
  public void mergeNStreams() {
    throw new NotImplementedException();
  }

  @Test
  public void mergeOneStream() {
    throw new NotImplementedException();
  }

  static class Application implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      LOG.debug("Application - PopulateDAG");
      RandomWordGenerator randomWordGenerator = new RandomWordGenerator();
      RandomWordGenerator randomWordGenerator2 = new RandomWordGenerator();
      RandomWordGenerator randomWordGenerator3 = new RandomWordGenerator();
      RandomWordGenerator randomWordGenerator4 = new RandomWordGenerator();

      randomWordGenerator.setTuplesPerWindow(1);
      randomWordGenerator2.setTuplesPerWindow(1);
      randomWordGenerator3.setTuplesPerWindow(1);
      randomWordGenerator4.setTuplesPerWindow(1);

      dag.addOperator("RandomWords", randomWordGenerator);
      dag.addOperator("RandomWords2", randomWordGenerator2);
      dag.addOperator("RandomWords3", randomWordGenerator3);
      dag.addOperator("RandomWords4", randomWordGenerator4);

      MultipleStreamMerger<byte[]> merger = new MultipleStreamMerger<>();
      merger.merge(randomWordGenerator.output)
          .merge(randomWordGenerator2.output)
          .merge(randomWordGenerator3.output)
          .merge(randomWordGenerator4.output)
          .insertInto(dag, conf);

      // This should connect all the relevant ports
      Module m1 = dag.addModule("Merger", merger);


      // And then we should see the output
      ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
      dag.addStream("merger-console", merger.streamOutput, consoleOperator.input);
    }
  }

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}