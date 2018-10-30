/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.adapter;

import java.io.IOException;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public interface RowMergingDataAdapter<T, M extends Mergeable> extends
		DataTypeAdapter<T>
{
	public RowTransform<M> getTransform();

	public Map<String, String> getOptions(
			short internalAdapterId,
			Map<String, String> existingOptions );

	public static interface RowTransform<M extends Mergeable> extends
			Persistable
	{
		public void initOptions(
				final Map<String, String> options )
				throws IOException;

		public M getRowAsMergeableObject(
				final short internalAdapterId,
				final ByteArray fieldId,
				final byte[] rowValueBinary );

		public byte[] getBinaryFromMergedObject(
				final M rowObject );

		public String getTransformName();

		public int getBaseTransformPriority();
	}
}
