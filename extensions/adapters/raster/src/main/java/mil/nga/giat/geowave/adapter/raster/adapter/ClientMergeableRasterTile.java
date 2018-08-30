package mil.nga.giat.geowave.adapter.raster.adapter;

import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.ServerMergeStrategy;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.Persistable;

public class ClientMergeableRasterTile<T extends Persistable> extends
		RasterTile<T>
{
	private RasterTileMergeStrategy<T> mergeStrategy;
	private SampleModel sampleModel;

	public ClientMergeableRasterTile() {}

	public ClientMergeableRasterTile(
			RasterTileMergeStrategy<T> mergeStrategy,
			SampleModel sampleModel,
			final DataBuffer dataBuffer,
			final T metadata ) {
		super(
				dataBuffer,
				metadata);
		this.mergeStrategy = mergeStrategy;

		this.sampleModel = sampleModel;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((mergeStrategy != null) && (merge != null) && (merge instanceof RasterTile)) {
			mergeStrategy.merge(
					this,
					(RasterTile<T>) merge,
					sampleModel);
		}
		else {
			super.merge(merge);
		}
	}
}
