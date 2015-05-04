package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.hdfs.AbstractStageFileToHdfs;
import mil.nga.giat.geowave.core.ingest.hdfs.HdfsFile;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;

import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

/**
 * This class can be sub-classed as a general-purpose recipe for parallelizing
 * ingestion of files either locally or by directly staging the binary of the
 * file to HDFS and then ingesting it within the map phase of a map-reduce job.
 */
abstract public class AbstractLocalIngestWithMapper<T> extends
		AbstractStageFileToHdfs implements
		LocalFileIngestPlugin<T>,
		IngestFromHdfsPlugin<HdfsFile, T>
{
	private final static Logger LOGGER = Logger.getLogger(AbstractLocalIngestWithMapper.class);

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<HdfsFile, T> ingestWithMapper() {
		return new InternalIngestWithMapper<T>(
				this);
	}

	@Override
	public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		try (final InputStream inputStream = new FileInputStream(
				input)) {
			return toGeoWaveDataInternal(
					inputStream,
					primaryIndexId,
					globalVisibility);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Cannot open file, unable to ingest",
					e);
		}
		return new CloseableIterator.Wrapper(
				Iterators.emptyIterator());
	}

	abstract protected CloseableIterator<GeoWaveData<T>> toGeoWaveDataInternal(
			final InputStream file,
			final ByteArrayId primaryIndexId,
			final String globalVisibility );

	@Override
	public IngestWithReducer<HdfsFile, ?, ?, T> ingestWithReducer() {
		return null;
	}

	private static class InternalIngestWithMapper<T> implements
			IngestWithMapper<HdfsFile, T>
	{
		private AbstractLocalIngestWithMapper parentPlugin;

		public InternalIngestWithMapper() {}

		public InternalIngestWithMapper(
				final AbstractLocalIngestWithMapper parentPlugin ) {
			this.parentPlugin = parentPlugin;
		}

		@Override
		public WritableDataAdapter<T>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<T>> toGeoWaveData(
				final HdfsFile input,
				final ByteArrayId primaryIndexId,
				final String globalVisibility ) {
			final InputStream inputStream = new ByteBufferBackedInputStream(
					input.getOriginalFile());
			return parentPlugin.toGeoWaveDataInternal(
					inputStream,
					primaryIndexId,
					globalVisibility);
		}

		@Override
		public byte[] toBinary() {
			return StringUtils.stringToBinary(parentPlugin.getClass().getName());
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			parentPlugin = PersistenceUtils.classFactory(
					StringUtils.stringFromBinary(bytes),
					AbstractLocalIngestWithMapper.class);
		}
	}

}
