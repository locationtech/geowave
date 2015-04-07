package mil.nga.giat.geowave.types;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.ingest.hdfs.HdfsFile;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.types.geolife.GeoLifeIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

abstract public class AbstractSimpleFeatureIngestPlugin<I> implements
		LocalFileIngestPlugin<SimpleFeature>,
		IngestFromHdfsPlugin<I, SimpleFeature>,
		StageToHdfsPlugin<I>,
		Persistable
{
	protected CQLFilterOptionProvider filterProvider = new CQLFilterOptionProvider();

	public void setFilterProvider(
			CQLFilterOptionProvider filterProvider ) {
		this.filterProvider = filterProvider;
	}

	@Override
	public byte[] toBinary() {
		return filterProvider.toBinary();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		filterProvider = new CQLFilterOptionProvider();
		filterProvider.fromBinary(bytes);
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final I[] hdfsObjects = toHdfsObjects(input);
		final List<CloseableIterator<GeoWaveData<SimpleFeature>>> allData = new ArrayList<CloseableIterator<GeoWaveData<SimpleFeature>>>();
		for (final I hdfsObject : hdfsObjects) {
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData = toGeoWaveDataInternal(
					hdfsObject,
					primaryIndexId,
					globalVisibility);
			if (filterProvider != null) {
				final Iterator<GeoWaveData<SimpleFeature>> it = Iterators.filter(
						geowaveData,
						new Predicate<GeoWaveData<SimpleFeature>>() {
							@Override
							public boolean apply(
									final GeoWaveData<SimpleFeature> input ) {
								return filterProvider.evaluate(input.getValue());
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
	}
}
