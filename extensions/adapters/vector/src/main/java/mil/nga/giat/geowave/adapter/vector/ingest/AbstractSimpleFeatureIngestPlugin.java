package mil.nga.giat.geowave.adapter.vector.ingest;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.adapter.vector.AvroFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.WholeFeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
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

	@Override
	public byte[] toBinary() {
		final byte[] filterBinary = filterOptionProvider.toBinary();
		final byte[] typeNameBinary = typeNameProvider.toBinary();
		final ByteBuffer buf = ByteBuffer.allocate(filterBinary.length + typeNameBinary.length + 4);
		buf.putInt(filterBinary.length);
		buf.put(filterBinary);
		buf.put(typeNameBinary);
		return ArrayUtils.addAll(
				serializationFormatOptionProvider.toBinary(),
				buf.array());
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
		final byte[] typeNameBinary = new byte[otherBytes.length - filterBinaryLength - 4];
		buf.get(filterBinary);
		buf.get(typeNameBinary);

		serializationFormatOptionProvider = new FeatureSerializationOptionProvider();
		serializationFormatOptionProvider.fromBinary(kryoBytes);

		filterOptionProvider = new CQLFilterOptionProvider();
		filterOptionProvider.fromBinary(filterBinary);

		typeNameProvider = new TypeNameOptionProvider();
		typeNameProvider.fromBinary(typeNameBinary);
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
			final File input,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		final I[] hdfsObjects = toAvroObjects(input);
		final List<CloseableIterator<GeoWaveData<SimpleFeature>>> allData = new ArrayList<CloseableIterator<GeoWaveData<SimpleFeature>>>();

		for (final I hdfsObject : hdfsObjects) {
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData = toGeoWaveDataInternal(
					hdfsObject,
					primaryIndexIds,
					globalVisibility);
			allData.add(wrapIteratorWithFilters(geowaveData));
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				Iterators.concat(allData.iterator()));
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
