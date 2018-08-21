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
package mil.nga.giat.geowave.adapter.vector.ingest;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.AvroFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

abstract public class AbstractSimpleFeatureIngestPlugin<I> implements
		LocalFileIngestPlugin<SimpleFeature>,
		IngestFromHdfsPlugin<I, SimpleFeature>,
		AvroFormatPlugin<I, SimpleFeature>,
		Persistable
{
	protected CQLFilterOptionProvider filterOptionProvider = new CQLFilterOptionProvider();
	protected FeatureSerializationOptionProvider serializationFormatOptionProvider = new FeatureSerializationOptionProvider();
	protected TypeNameOptionProvider typeNameProvider = new TypeNameOptionProvider();
	protected GeometrySimpOptionProvider simpOptionProvider = new GeometrySimpOptionProvider();

	public void setFilterProvider(
			final CQLFilterOptionProvider filterOptionProvider ) {
		this.filterOptionProvider = filterOptionProvider;
	}

	public void setSerializationFormatProvider(
			final FeatureSerializationOptionProvider serializationFormatOptionProvider ) {
		this.serializationFormatOptionProvider = serializationFormatOptionProvider;
	}

	public void setTypeNameProvider(
			final TypeNameOptionProvider typeNameProvider ) {
		this.typeNameProvider = typeNameProvider;
	}

	public void setGeometrySimpOptionProvider(
			final GeometrySimpOptionProvider geometryProvider ) {
		this.simpOptionProvider = geometryProvider;
	}

	@Override
	public byte[] toBinary() {
		final byte[] filterBinary = filterOptionProvider.toBinary();
		final byte[] typeNameBinary = typeNameProvider.toBinary();
		final byte[] simpBinary = simpOptionProvider.toBinary();
		final byte[] backingBuffer = new byte[filterBinary.length + typeNameBinary.length + simpBinary.length
				+ (Integer.BYTES * 2)];
		final ByteBuffer buf = ByteBuffer.wrap(backingBuffer);
		buf.putInt(filterBinary.length);
		buf.put(filterBinary);
		buf.putInt(typeNameBinary.length);
		buf.put(typeNameBinary);
		buf.put(simpBinary);

		return ArrayUtils.addAll(
				serializationFormatOptionProvider.toBinary(),
				backingBuffer);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final byte[] otherBytes = new byte[bytes.length - 1];
		System.arraycopy(
				bytes,
				1,
				otherBytes,
				0,
				otherBytes.length);
		final byte[] kryoBytes = new byte[] {
			bytes[0]
		};
		final ByteBuffer buf = ByteBuffer.wrap(otherBytes);
		final int filterBinaryLength = buf.getInt();
		final byte[] filterBinary = new byte[filterBinaryLength];
		buf.get(filterBinary);

		final int typeNameBinaryLength = buf.getInt();
		final byte[] typeNameBinary = new byte[typeNameBinaryLength];
		buf.get(typeNameBinary);

		final byte[] geometrySimpBinary = new byte[otherBytes.length - filterBinary.length - typeNameBinary.length
				- (Integer.BYTES * 2)];
		buf.get(geometrySimpBinary);

		serializationFormatOptionProvider = new FeatureSerializationOptionProvider();
		serializationFormatOptionProvider.fromBinary(kryoBytes);

		filterOptionProvider = new CQLFilterOptionProvider();
		filterOptionProvider.fromBinary(filterBinary);

		typeNameProvider = new TypeNameOptionProvider();
		typeNameProvider.fromBinary(typeNameBinary);

		simpOptionProvider = new GeometrySimpOptionProvider();
		simpOptionProvider.fromBinary(geometrySimpBinary);
	}

	protected WritableDataAdapter<SimpleFeature> newAdapter(
			final SimpleFeatureType type,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		if (serializationFormatOptionProvider.isAvro()) {
			return new AvroFeatureDataAdapter(
					type);
		}
		return new FeatureDataAdapter(
				type,
				fieldVisiblityHandler);
	}

	abstract protected SimpleFeatureType[] getTypes();

	@Override
	public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
			final String globalVisibility ) {
		final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler = ((globalVisibility != null) && !globalVisibility
				.isEmpty()) ? new GlobalVisibilityHandler<SimpleFeature, Object>(
				globalVisibility) : null;
		final SimpleFeatureType[] types = getTypes();
		final WritableDataAdapter<SimpleFeature>[] retVal = new WritableDataAdapter[types.length];
		for (int i = 0; i < types.length; i++) {
			retVal[i] = newAdapter(
					types[i],
					fieldVisiblityHandler);
		}
		return retVal;
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final URL input,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		final CloseableIterator<I> hdfsObjects = toAvroObjects(input);
		return new CloseableIterator<GeoWaveData<SimpleFeature>>() {

			CloseableIterator<GeoWaveData<SimpleFeature>> currentIterator = null;
			GeoWaveData<SimpleFeature> next = null;

			private void computeNext() {
				try {
					if (next == null) {
						if (currentIterator != null) {
							if (currentIterator.hasNext()) {
								next = currentIterator.next();
								return;
							}
							else {
								currentIterator.close();
								currentIterator = null;
							}
						}
						while (hdfsObjects.hasNext()) {
							final I hdfsObject = hdfsObjects.next();
							currentIterator = toGeoWaveDataInternal(
									hdfsObject,
									primaryIndexIds,
									globalVisibility);
							if (currentIterator.hasNext()) {
								next = currentIterator.next();
								return;
							}
							else {
								currentIterator.close();
								currentIterator = null;
							}
						}
					}
				}
				catch (IOException e) {
					Throwables.propagate(e);
				}
			}

			@Override
			public boolean hasNext() {
				computeNext();
				return next != null;
			}

			@Override
			public GeoWaveData<SimpleFeature> next() {
				computeNext();
				GeoWaveData<SimpleFeature> retVal = next;
				next = null;
				return retVal;
			}

			@Override
			public void close()
					throws IOException {
				hdfsObjects.close();
			}

		};
	}

	protected CloseableIterator<GeoWaveData<SimpleFeature>> wrapIteratorWithFilters(
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData ) {
		final CQLFilterOptionProvider internalFilterProvider;
		if ((filterOptionProvider != null) && (filterOptionProvider.getCqlFilterString() != null)
				&& !filterOptionProvider.getCqlFilterString().trim().isEmpty()) {
			internalFilterProvider = filterOptionProvider;
		}
		else {
			internalFilterProvider = null;
		}
		final TypeNameOptionProvider internalTypeNameProvider;
		if ((typeNameProvider != null) && (typeNameProvider.getTypeName() != null)
				&& !typeNameProvider.getTypeName().trim().isEmpty()) {
			internalTypeNameProvider = typeNameProvider;
		}
		else {
			internalTypeNameProvider = null;
		}
		final GeometrySimpOptionProvider internalSimpOptionProvider;
		if ((simpOptionProvider != null)) {
			internalSimpOptionProvider = simpOptionProvider;
		}
		else {
			internalSimpOptionProvider = null;
		}
		if ((internalFilterProvider != null) || (internalTypeNameProvider != null)) {
			final Iterator<GeoWaveData<SimpleFeature>> it = Iterators.filter(
					geowaveData,
					new Predicate<GeoWaveData<SimpleFeature>>() {
						@Override
						public boolean apply(
								final GeoWaveData<SimpleFeature> input ) {
							if ((internalTypeNameProvider != null)
									&& !internalTypeNameProvider.typeNameMatches(input.getAdapterId().getString())) {
								return false;
							}
							if ((internalFilterProvider != null) && !internalFilterProvider.evaluate(input.getValue())) {
								return false;
							}
							if ((internalSimpOptionProvider != null)) {
								Geometry simpGeom = internalSimpOptionProvider.simplifyGeometry((Geometry) input
										.getValue()
										.getDefaultGeometry());
								if (!internalSimpOptionProvider.filterGeometry(simpGeom)) {
									return false;
								}
								input.getValue().setDefaultGeometry(
										simpGeom);
							}
							return true;
						}
					});
			return new CloseableIteratorWrapper<GeoWaveData<SimpleFeature>>(
					geowaveData,
					it);
		}
		return geowaveData;
	}

	abstract protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final I hdfsObject,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility );

	abstract public static class AbstractIngestSimpleFeatureWithMapper<I> implements
			IngestWithMapper<I, SimpleFeature>
	{
		protected AbstractSimpleFeatureIngestPlugin<I> parentPlugin;

		public AbstractIngestSimpleFeatureWithMapper(
				final AbstractSimpleFeatureIngestPlugin<I> parentPlugin ) {
			this.parentPlugin = parentPlugin;
		}

		@Override
		public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
				final I input,
				final Collection<ByteArrayId> primaryIndexIds,
				final String globalVisibility ) {
			return parentPlugin.wrapIteratorWithFilters(parentPlugin.toGeoWaveDataInternal(
					input,
					primaryIndexIds,
					globalVisibility));
		}

		@Override
		public byte[] toBinary() {
			return parentPlugin.toBinary();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			parentPlugin.fromBinary(bytes);
		}

		@Override
		public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
			return parentPlugin.getSupportedIndexableTypes();
		}
	}
}
