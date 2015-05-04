package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.datastore.accumulo.MergingVisibilityCombiner;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;

public class RasterTileVisibilityCombiner extends
		MergingVisibilityCombiner
{
	private final RasterTileCombinerHelper<Persistable> helper = new RasterTileCombinerHelper<Persistable>();
	private ColumnSet columns;

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {
		if (input.hasTop() && columns.contains(input.getTopKey())) {
			super.transformRange(
					input,
					output);
		}
		else {
			while (input.hasTop()) {
				output.append(
						input.getTopKey(),
						input.getTopValue());
				input.next();
			}
		}
	}

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
		if (!options.containsKey(RasterTileCombiner.COLUMNS_KEY)) {
			throw new IllegalArgumentException(
					"Must specify " + RasterTileCombiner.COLUMNS_KEY + " option");
		}

		final String encodedColumns = options.get(RasterTileCombiner.COLUMNS_KEY);
		if (encodedColumns.length() == 0) {
			throw new IllegalArgumentException(
					"The " + RasterTileCombiner.COLUMNS_KEY + " must not be empty");
		}

		columns = new ColumnSet(
				Arrays.asList(encodedColumns.split(",")));
		helper.init(options);
	}

}
