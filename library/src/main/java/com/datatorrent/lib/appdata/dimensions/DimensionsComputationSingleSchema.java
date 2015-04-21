/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;

import java.util.List;

public abstract class DimensionsComputationSingleSchema<INPUT_EVENT> extends DimensionsComputation<INPUT_EVENT>
{
  public static final int DEFAULT_SCHEMA_ID = 0;

  @NotNull
  private String eventSchemaJSON;
  private transient DimensionalEventSchema eventSchema;

  public DimensionsComputationSingleSchema()
  {
  }

  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = Preconditions.checkNotNull(eventSchemaJSON, "eventSchemaJSON");
  }

  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    eventSchema = new DimensionalEventSchema(eventSchemaJSON,
                                             getAggregatorInfo());
  }

  @Override
  public void convertInputEvent(INPUT_EVENT inputEvent, List<AggregateEvent> aggregateEventBuffer)
  {
    List<FieldsDescriptor> keyFieldsDescriptors = eventSchema.getDdIDToKeyDescriptor();

    for(int index = 0;
        index < keyFieldsDescriptors.size();
        index++) {
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(index);
      Int2ObjectMap<FieldsDescriptor> map = eventSchema.getDdIDToAggIDToInputAggDescriptor().get(index);
      IntArrayList aggIDList = eventSchema.getDdIDToAggIDs().get(index);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++) {
        int aggID = aggIDList.get(aggIDIndex);
        aggregateEventBuffer.add(createGenericAggregateEvent(inputEvent,
                                                             eventSchema.getDdIDToDD().get(index),
                                                             keyFieldsDescriptor,
                                                             map.get(aggID),
                                                             index,
                                                             aggID));
      }
    }
  }

  @Override
  public FieldsDescriptor getAggregateFieldsDescriptor(int schemaID, int dimensionDescriptorID, int aggregatorID)
  {
    return eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionDescriptorID).get(aggregatorID);
  }

  public abstract AggregateEvent createGenericAggregateEvent(INPUT_EVENT inputEvent,
                                                             DimensionsDescriptor dd,
                                                             FieldsDescriptor keyFieldsDescriptor,
                                                             FieldsDescriptor aggregateDescriptor,
                                                             int dimensionDescriptorID,
                                                             int aggregateID);
}
