package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.datastore.accumulo.MergingCombiner;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class RasterTileCombiner extends
		MergingCombiner
{
	public static final String COLUMNS_KEY = COLUMNS_OPTION;
	private final RasterTileCombinerHelper<Persistable> helper = new RasterTileCombinerHelper<Persistable>();

	@Override
	protected Mergeable getMergeable(
			final Key key,
			final byte[] binary ) {
		final RasterTile mergeable = PersistenceUtils.classFactory(
				RasterTile.class.getName(),
				RasterTile.class);

		if (mergeable != null) {
			mergeable.fromBinary(binary);
		}
		return helper.transform(
				key,
				mergeable);
	}

	@Override
	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return mergeable.toBinary();
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		helper.init(options);
	}
}
