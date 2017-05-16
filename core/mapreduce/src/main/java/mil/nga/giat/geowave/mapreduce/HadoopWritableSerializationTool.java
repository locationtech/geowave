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
package mil.nga.giat.geowave.mapreduce;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

/**
 * Use this class to maintain a set of serializers per adapters associated with
 * the context of a single mapper or reducer. The intent is to support
 * maintaining single set of Writable instances. By the nature of holding single
 * instances of Writable instances by the serializers, this class and its
 * contents may be only accessed by one 'worker' (at a time).
 * 
 * The helper methods assume all Writable instances are wrapped in an
 * ObjectWritable. The reason for this approach, consistent with other support
 * classes in this package, is to allow mappers and reducers to use the generic
 * ObjectWritable since entry inputs maybe be associated with different
 * adapters, and thus have different associated Writable instances.
 * Configuration of Hadoop Mappers and Reducers requires a specific type.
 * 
 */
public class HadoopWritableSerializationTool
{
	private final AdapterStore adapterStore;
	private final Map<ByteArrayId, HadoopWritableSerializer<Object, Writable>> serializers = new HashMap<ByteArrayId, HadoopWritableSerializer<Object, Writable>>();
	private final ObjectWritable objectWritable = new ObjectWritable();

	public HadoopWritableSerializationTool(
			final AdapterStore adapterStore ) {
		super();
		this.adapterStore = adapterStore;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}

	public DataAdapter<?> getAdapter(
			final ByteArrayId adapterId ) {
		return adapterStore.getAdapter(adapterId);
	}

	public HadoopWritableSerializer<Object, Writable> getHadoopWritableSerializerForAdapter(
			final ByteArrayId adapterID ) {

		HadoopWritableSerializer<Object, Writable> serializer = serializers.get(adapterID);
		if (serializer == null) {
			DataAdapter<?> adapter;
			if ((adapterStore != null) && ((adapter = adapterStore.getAdapter(adapterID)) != null)
					&& (adapter instanceof HadoopDataAdapter)) {
				serializer = ((HadoopDataAdapter<Object, Writable>) adapter).createWritableSerializer();
				serializers.put(
						adapterID,
						serializer);
			}
			else {
				serializer = new HadoopWritableSerializer<Object, Writable>() {
					final ObjectWritable writable = new ObjectWritable();

					@Override
					public ObjectWritable toWritable(
							final Object entry ) {
						writable.set(entry);
						return writable;
					}

					@Override
					public Object fromWritable(
							final Writable writable ) {
						return ((ObjectWritable) writable).get();
					}
				};
			}
		}
		return serializer;
	}

	public ObjectWritable toWritable(
			final ByteArrayId adapterID,
			final Object entry ) {
		if (entry instanceof Writable) {
			objectWritable.set(entry);
		}
		else {
			objectWritable.set(getHadoopWritableSerializerForAdapter(
					adapterID).toWritable(
					entry));
		}
		return objectWritable;
	}

	public Object fromWritable(
			final ByteArrayId adapterID,
			final ObjectWritable writable ) {
		final Object innerObj = writable.get();
		return (innerObj instanceof Writable) ? getHadoopWritableSerializerForAdapter(
				adapterID).fromWritable(
				(Writable) innerObj) : innerObj;
	}
}
