package mil.nga.giat.geowave.adapter.raster.adapter;

import java.awt.image.DataBuffer;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.RootMergeStrategy;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;

public class MergeableRasterTile<T extends Persistable> extends
		RasterTile<T>
{
	private final RootMergeStrategy<T> mergeStrategy;
	private final ByteArrayId dataAdapterId;

	public MergeableRasterTile(
			final DataBuffer dataBuffer,
			final T metadata,
			final RootMergeStrategy<T> mergeStrategy,
			final ByteArrayId dataAdapterId ) {
		super(
				dataBuffer,
				metadata);
		this.mergeStrategy = mergeStrategy;
		this.dataAdapterId = dataAdapterId;
	}

	public ByteArrayId getDataAdapterId() {
		return dataAdapterId;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((mergeStrategy != null) && (merge != null) && (merge instanceof RasterTile)) {
			mergeStrategy.merge(
					this,
					(RasterTile<T>) merge,
					dataAdapterId);
		}
	}
}
