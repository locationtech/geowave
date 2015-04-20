package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

public class RasterTileCombinerConfig extends
		IteratorConfig
{

	public RasterTileCombinerConfig(
			final IteratorSetting iteratorSettings,
			final EnumSet<IteratorScope> scopes ) {
		super(
				iteratorSettings,
				scopes);
	}

	@Override
	public String mergeOption(
			final String optionKey,
			final String currentValue,
			final String nextValue ) {
		if ((currentValue == null) || currentValue.trim().isEmpty()) {
			return nextValue;
		}
		else if ((nextValue == null) || nextValue.trim().isEmpty()) {
			return currentValue;
		}
		if (RasterTileCombinerHelper.MERGE_STRATEGY_KEY.equals(optionKey)) {
			final byte[] currentStrategyBytes = ByteArrayUtils.byteArrayFromString(currentValue);
			final byte[] nextStrategyBytes = ByteArrayUtils.byteArrayFromString(nextValue);
			final RootMergeStrategy currentStrategy = PersistenceUtils.fromBinary(
					currentStrategyBytes,
					RootMergeStrategy.class);
			final RootMergeStrategy nextStrategy = PersistenceUtils.fromBinary(
					nextStrategyBytes,
					RootMergeStrategy.class);
			currentStrategy.merge(nextStrategy);
			return ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(currentStrategy));
		}
		else if (RasterTileCombiner.COLUMNS_KEY.equals(optionKey)) {
			final String encodedColumns = currentValue;
			final Set<String> nextColumns = new HashSet<String>();
			for (final String column : nextValue.split(",")) {
				nextColumns.add(column);
			}
			final StringBuffer str = new StringBuffer(
					nextValue);
			for (final String column : encodedColumns.split(",")) {
				if (!nextColumns.contains(column)) {
					str.append(",");
					str.append(column);
				}
			}
			return str.toString();
		}
		return super.mergeOption(
				optionKey,
				currentValue,
				nextValue);
	}

}
