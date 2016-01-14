package mil.nga.giat.geowave.adapter.vector.ingest;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

import org.apache.commons.lang.ArrayUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

abstract public class AbstractSimpleFeatureIngestPlugin<I> implements
		LocalFileIngestPlugin<SimpleFeature>,
		IngestFromHdfsPlugin<I, SimpleFeature>,
		AvroFormatPlugin<I, SimpleFeature>,
		Persistable
{
	protected CQLFilterOptionProvider filterOptionProvider = new CQLFilterOptionProvider();
	protected FeatureSerializationOptionProvider serializationFormatOptionProvider = new FeatureSerializationOptionProvider();

	public void setFilterProvider(
			final CQLFilterOptionProvider filterOptionProvider ) {
		this.filterOptionProvider = filterOptionProvider;
	}

	public void setSerializationFormatProvider(
			final FeatureSerializationOptionProvider serializationFormatOptionProvider ) {
		this.serializationFormatOptionProvider = serializationFormatOptionProvider;
	}

	@Override
	public byte[] toBinary() {
		return ArrayUtils.addAll(
				serializationFormatOptionProvider.toBinary(),
				filterOptionProvider.toBinary());
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final byte[] cqlBytes = new byte[bytes.length - 1];
		System.arraycopy(
				bytes,
				1,
				cqlBytes,
				0,
				cqlBytes.length);
		final byte[] kryoBytes = new byte[] {
			bytes[0]
		};
		serializationFormatOptionProvider = new FeatureSerializationOptionProvider();
		serializationFormatOptionProvider.fromBinary(kryoBytes);
		filterOptionProvider = new CQLFilterOptionProvider();
		filterOptionProvider.fromBinary(bytes);
	}

	protected WritableDataAdapter<SimpleFeature> newAdapter(
			final SimpleFeatureType type,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		// TODO: assign other adapters based on serialization option
		if (serializationFormatOptionProvider.isWhole()) {
			return new WholeFeatureDataAdapter(
					type);
		}
		else if (serializationFormatOptionProvider.isAvro()) {
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
		final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler = ((globalVisibility != null) && !globalVisibility.isEmpty()) ? new GlobalVisibilityHandler<SimpleFeature, Object>(
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
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final I[] hdfsObjects = toAvroObjects(input);
		final List<CloseableIterator<GeoWaveData<SimpleFeature>>> allData = new ArrayList<CloseableIterator<GeoWaveData<SimpleFeature>>>();
		for (final I hdfsObject : hdfsObjects) {
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData = toGeoWaveDataInternal(
					hdfsObject,
					primaryIndexId,
					globalVisibility);
			if (filterOptionProvider != null) {
				final Iterator<GeoWaveData<SimpleFeature>> it = Iterators.filter(
						geowaveData,
						new Predicate<GeoWaveData<SimpleFeature>>() {
							@Override
							public boolean apply(
									final GeoWaveData<SimpleFeature> input ) {
								return filterOptionProvider.evaluate(input.getValue());
							}
						});
				allData.add(new CloseableIteratorWrapper<GeoWaveData<SimpleFeature>>(
						geowaveData,
						it));
			}
			allData.add(geowaveData);
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				Iterators.concat(allData.iterator()));
	}

	abstract protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final I hdfsObject,
			final ByteArrayId primaryIndexId,
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
				final ByteArrayId primaryIndexId,
				final String globalVisibility ) {
			return parentPlugin.toGeoWaveDataInternal(
					input,
					primaryIndexId,
					globalVisibility);
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
