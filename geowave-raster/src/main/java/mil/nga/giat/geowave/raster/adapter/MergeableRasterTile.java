package mil.nga.giat.geowave.raster.adapter;

import java.awt.image.DataBuffer;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.raster.adapter.merge.RasterTileMergeStrategy;

public class MergeableRasterTile<T extends Persistable> extends
		RasterTile<T>
{
	private final RasterTileMergeStrategy<T> mergeStrategy;
	private final ByteArrayId dataAdapterId;

	public MergeableRasterTile(
			final DataBuffer dataBuffer,
			final T metadata,
			final RasterTileMergeStrategy<T> mergeStrategy,
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
					(RasterTile<T>) merge);
		}
	}
}
