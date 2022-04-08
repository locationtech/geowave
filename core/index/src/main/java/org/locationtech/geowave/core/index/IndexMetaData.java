/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public interface IndexMetaData extends Mergeable {
  /**
   * Update the aggregation result using the new entry provided
   *
   * @param insertionIds the new indices to compute an updated aggregation result on
   */
  public void insertionIdsAdded(InsertionIds insertionIds);

  /**
   * Update the aggregation result by removing the entries provided
   *
   * @param insertionIds the new indices to compute an updated aggregation result on
   */
  public void insertionIdsRemoved(InsertionIds insertionIds);

  /** Create a JSON object that shows all the metadata handled by this object */
  public JSONObject toJSONObject() throws JSONException;
}
