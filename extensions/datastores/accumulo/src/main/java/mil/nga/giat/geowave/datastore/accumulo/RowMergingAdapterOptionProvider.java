package mil.nga.giat.geowave.datastore.accumulo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig.OptionProvider;

import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.hadoop.io.Text;

public class RowMergingAdapterOptionProvider implements
		OptionProvider
{
	public static final String ROW_TRANSFORM_KEY = "ROW_TRANSFORM";
	public static final String ROW_MERGING_ADAPTER_CACHE_ID = "ROW_MERGING_ADAPTER";

	private final RowMergingDataAdapter<?, ?> adapter;

	public RowMergingAdapterOptionProvider(
			final RowMergingDataAdapter<?, ?> adapter ) {
		this.adapter = adapter;
	}

	@Override
	public Map<String, String> getOptions(
			final Map<String, String> existingOptions ) {
		final Map<String, String> newOptions = adapter.getOptions(existingOptions);

		final Column adapterColumn = new Column(
				new Text(
						adapter.getAdapterId().getBytes()));

		String nextColumnValue = ColumnSet.encodeColumns(
				adapterColumn.getFirst(),
				adapterColumn.getSecond());
		if ((existingOptions != null) && existingOptions.containsKey(MergingCombiner.COLUMNS_OPTION)) {
			final String encodedColumns = existingOptions.get(MergingCombiner.COLUMNS_OPTION);
			final Set<String> nextColumns = new HashSet<String>();
			for (final String column : nextColumnValue.split(",")) {
				nextColumns.add(column);
			}
			final StringBuffer str = new StringBuffer(
					nextColumnValue);
			for (final String column : encodedColumns.split(",")) {
				if (!nextColumns.contains(column)) {
					str.append(",");
					str.append(column);
				}
			}
			nextColumnValue = str.toString();
		}
		newOptions.put(
				MergingCombiner.COLUMNS_OPTION,
				nextColumnValue);
		newOptions.put(
				ROW_TRANSFORM_KEY,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(adapter.getTransform())));
		return newOptions;
	}
}
