/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.stream;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Module;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Module that adds functionality to bypass the platform limitations of combining more than two streams at a time with
 * Stream Merger.
 *
 * Usage:
 *
 *    dag.addOperator("Stream_1", op1);
 *    dag.addOperator("Stream_2", op2);
 *    dag.addOperator("Stream_3", op3);
 *
 *    MultipleStreamMerger merger = new MultipleStreamMerger();
 *    merger.merge(op1.out)
 *          .merge(op2.out)
 *          .merge(op3.out);
 *
 *    dag.addModule("Merger", merger);

 * @param <K>
 */
public class MultipleStreamMerger<K> implements Module
{
  private int streamCount = 0;

  ArrayList<DefaultOutputPort<K>> streamsToMerge = new ArrayList<>();

  public transient ProxyOutputPort<K> streamOutput = new ProxyOutputPort<>();

  /**
   * Used to define all the sources to be merged into a single stream.
   *
   * @param sourcePort - The output port from the upstream operator that provides data
   * @return The updated MultipleStreamMerger object that tracks which streams should be unified.
   */
  public MultipleStreamMerger<K> merge(DefaultOutputPort<K> sourcePort)
  {
    streamsToMerge.add(sourcePort);
    return this;
  }

  /**
   * To merge more than two streams at a time, we construct a binary tree of thread-local StreamMerger operators
   * E.g.
   *
   *  Tier 0          Tier 1              Tier 2
   *
   * Stream 1 ->
   *              StreamMerger_1 ->
   * Stream 2 ->
   *                                StreamMerger_Final -> Out
   * Stream 3 ->
   *              StreamMerger_2 ->
   * Stream 4 ->
   *
   * This function updates the provided DAG with the relevant streams.
   */
  public void mergeStreams(DAG dag, Configuration conf, StreamMerger finalMerger)
  {
    if (streamsToMerge.size() < 2) {
      throw new IllegalArgumentException("Not enough streams to merge, at least two streams must be selected for " +
          "merging with `.merge()`.");
    }

    ArrayList<ArrayList<StreamMerger<K>>> mergers = new ArrayList<>();

    /**
     * First, calculate the number of tiers we need to merge all streams given that each merger can only merge two
     * streams at a time.
     */
    int numTiers = (int)Math.ceil(Math.log(streamsToMerge.size()) / Math.log(2));

    // Handle the simple case where we only have a single tier (only two streams to merge)
    if (numTiers == 1) {
      assert (streamsToMerge.size() == 2);
      dag.addStream("FinalMerge_Stream_0", streamsToMerge.get(0), finalMerger.data1);
      dag.addStream("FinalMerge_Stream_1", streamsToMerge.get(1), finalMerger.data2);
      return;
    }

    Iterator<DefaultOutputPort<K>> streams = streamsToMerge.iterator();

    // When assigning streams, we will switch between ports 1 and 2 as we use successive mergers.
    boolean usePort1;

    // For each tier, create the mergers in that tier, and connect the relevant streams
    for (int i = 0; i < numTiers -1; i++) {
      int streamIdx = 0;
      usePort1 = true;

      int numMergers = (int)Math.ceil(streamsToMerge.size() / Math.pow(2, i + 1));

      ArrayList<StreamMerger<K>> mergersTierI = new ArrayList<>(numMergers);

      // For each merger in the tier, assign the appropriate streams to that merger
      for (int mergerIdx = 0; mergerIdx < numMergers; mergerIdx++) {
        StreamMerger<K> merger = new StreamMerger<>();
        dag.addOperator("Merger_Tier_" + i + "_#_" + mergerIdx, merger);

        // Each operator has two ports so add a simple inner loop
        for (int port = 0; port < 2; port++) {
          /**
           * Assign streams. At the first tier, we assign the streams from "streamsToMerger". On successive tiers,
           * we assign streams from the previous tier.
           */
          DefaultInputPort<K> mergerInputPort = usePort1 ? merger.data1 : merger.data2;
          usePort1 = !usePort1;

          // Process first tier
          if (i == 0) {
            if (streams.hasNext()) {
              DefaultOutputPort<K> nextStream = streams.next();
              dag.addStream("Stream_" + streamIdx + " -> Tier_" + i + "_Merger_" + mergerIdx, nextStream, mergerInputPort)
                  .setLocality(DAG.Locality.CONTAINER_LOCAL);
            }
          } else {
            // Process subsequent tiers
            ArrayList<StreamMerger<K>> previousTier = mergers.get(i-1);
            if (streamIdx < previousTier.size()) {
              DefaultOutputPort<K> nextStream = previousTier.get(streamIdx).out;
              dag.addStream("Tier" + (i-1) + "_Stream_" + streamIdx + " -> Tier_" + i, nextStream, mergerInputPort)
                  .setLocality(DAG.Locality.CONTAINER_LOCAL);
            }
          }

          streamIdx++;
        }

        mergersTierI.add(merger);
      } // End tier loop

      mergers.add(mergersTierI);
    } // End cross-tier loop

    // We've now added streams connecting the input streams to cascading mergers. Lastly, we need to connect the final
    // tier to the output stream merger.

    ArrayList<StreamMerger<K>> finalTier = mergers.get(mergers.size() - 1);

    // If we're here, we're guaranteed to have had more than 2 streams to merge, so the final tier must contain
    // two merge operators
    assert (finalTier.size() == 2);

    dag.addStream("FinalMerge_0", finalTier.get(0).out, finalMerger.data1);
    dag.addStream("FinalMerge_1", finalTier.get(1).out, finalMerger.data2);
  }

  /**
   * Given the streams to merge have been selected with {@link #merge(DefaultOutputPort)}, create a subDAG and add it
   * to an existing DAG.
   *
   * To merge more than two streams at a time, we construct a tiered hierarchy of thread-local StreamMerger operators
   * E.g.
   *
   * Stream 1 ->
   *              StreamMerger_1 ->
   * Stream 2 ->
   *                                StreamMerger_Final -> Out
   * Stream 3 ->
   *              StreamMerger_2 ->
   * Stream 4 ->
   *
   * @param dag
   * @param conf
   *
   * Note that we don't use the populateDAG function because that is only used to flatten the module when what we really
   * need to do is to define the connections to the parent DAG. The populateDAG does not contain operators outside of
   * the module and thus cannot connect to the external operators.
   */
  public void insertInto(DAG dag, Configuration conf) {
    // Set the output of the module to come from the final merger
    StreamMerger<K> finalMerger = new StreamMerger<>();
    dag.addOperator("Merger_Final", finalMerger);
    streamOutput.set(finalMerger.out);

    mergeStreams(dag, conf, finalMerger);
  }

  /**

   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

  }
}
