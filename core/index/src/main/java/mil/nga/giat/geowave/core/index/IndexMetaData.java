/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.index;

import java.util.List;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public interface IndexMetaData extends
		Mergeable
{
	/**
	 * Update the aggregation result using the new entry provided
	 *
	 * @param insertionIds
	 *            the new indices to compute an updated aggregation result on
	 */
	public void insertionIdsAdded(
			List<ByteArrayId> insertionIds );

	/**
	 * Update the aggregation result by removing the entries provided
	 *
	 * @param insertionIds
	 *            the new indices to compute an updated aggregation result on
	 */
	public void insertionIdsRemoved(
			List<ByteArrayId> insertionIds );

	/**
	 * Create a JSON object that shows all the metadata handled by this object
	 * 
	 */
	public JSONObject toJSONObject()
			throws JSONException;

}
