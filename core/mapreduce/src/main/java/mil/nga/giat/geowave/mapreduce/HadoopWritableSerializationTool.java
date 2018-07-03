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

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

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
	private final TransientAdapterStore adapterStore;
	private final InternalAdapterStore internalAdapterStore;
	private final Map<ByteArrayId, HadoopWritableSerializer<Object, Writable>> serializers = new HashMap<ByteArrayId, HadoopWritableSerializer<Object, Writable>>();
	private final ObjectWritable objectWritable = new ObjectWritable();

	public HadoopWritableSerializationTool(
			final JobContext jobContext ) {
		this(
				GeoWaveInputFormat.getJobContextAdapterStore(jobContext),
				GeoWaveInputFormat.getJobContextInternalAdapterStore(jobContext));
	}

	public HadoopWritableSerializationTool(
			final TransientAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore ) {
		super();
		this.adapterStore = adapterStore;
		this.internalAdapterStore = internalAdapterStore;
	}

	public TransientAdapterStore getAdapterStore() {
		return adapterStore;
	}

	public InternalDataAdapter<?> getInternalAdapter(
			final short internalAdapterId ) {
		return new InternalDataAdapterWrapper(
				(WritableDataAdapter) adapterStore.getAdapter(internalAdapterStore.getAdapterId(internalAdapterId)),
				internalAdapterId);
	}

	public DataAdapter<?> getAdapter(
			final ByteArrayId adapterId ) {
		return adapterStore.getAdapter(adapterId);
	}

	public HadoopWritableSerializer<Object, Writable> getHadoopWritableSerializerForAdapter(
			final short internalAdapterId ) {
		return getHadoopWritableSerializerForAdapter(internalAdapterStore.getAdapterId(internalAdapterId));
	}

	public HadoopWritableSerializer<Object, Writable> getHadoopWritableSerializerForAdapter(
			final ByteArrayId adapterId ) {

		HadoopWritableSerializer<Object, Writable> serializer = serializers.get(adapterId);
		if (serializer == null) {
			DataAdapter<?> adapter;

			HadoopDataAdapter<Object, Writable> hadoopAdapter = null;
			if (((adapterStore != null) && ((adapter = adapterStore.getAdapter(adapterId)) != null))) {
				if (adapter instanceof HadoopDataAdapter) {
					hadoopAdapter = (HadoopDataAdapter<Object, Writable>) adapter;
				}
				else if ((adapter instanceof InternalDataAdapter)
						&& (((InternalDataAdapter) adapter).getAdapter() instanceof HadoopDataAdapter)) {
					hadoopAdapter = (HadoopDataAdapter<Object, Writable>) ((InternalDataAdapter) adapter).getAdapter();
				}
			}
			if (hadoopAdapter != null) {
				serializer = hadoopAdapter.createWritableSerializer();
				serializers.put(
						adapterId,
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
			final short internalAdapterId,
			final Object entry ) {
		if (entry instanceof Writable) {
			objectWritable.set(entry);
		}
		else {
			objectWritable.set(getHadoopWritableSerializerForAdapter(
					internalAdapterId).toWritable(
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

	public Object fromWritable(
			final short internalAdapterId,
			final ObjectWritable writable ) {
		final Object innerObj = writable.get();
		return (innerObj instanceof Writable) ? getHadoopWritableSerializerForAdapter(
				internalAdapterId).fromWritable(
				(Writable) innerObj) : innerObj;
	}
}
