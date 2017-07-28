package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.core.store.server.RowMergingAdapterOptionProvider;

public class RowMergingServerOp extends
		MergingServerOp
{
	private RowTransform<Mergeable> rowTransform;

	@Override
	protected Mergeable getMergeable(
			final Cell cell,
			final byte[] bytes ) {
		return rowTransform.getRowAsMergeableObject(
				new ByteArrayId(
						CellUtil.cloneFamily(cell)),
				new ByteArrayId(
						CellUtil.cloneQualifier(cell)),
				bytes);
	}

	@Override
	protected String getColumnOptionValue(
			final Map<String, String> options ) {
		// if this is "row" merging than it is by adapter ID
		return options.get(RowMergingAdapterOptionProvider.ADAPTER_IDS_OPTION);
	}

	@Override
	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return rowTransform.getBinaryFromMergedObject(mergeable);
	}

	@Override
	public void init(
			final Map<String, String> options )
			throws IOException {
		final String columnStr = options.get(RowMergingAdapterOptionProvider.ADAPTER_IDS_OPTION);

		if (columnStr.length() == 0) {
			throw new IllegalArgumentException(
					"The column must not be empty");
		}
		columnFamilyIds = Sets.newHashSet(Iterables.transform(
				Splitter.on(
						",").split(
						columnStr),
				new Function<String, ByteArrayId>() {

					@Override
					public ByteArrayId apply(
							final String input ) {
						return new ByteArrayId(
								input);
					}
				}));
		final String rowTransformStr = options.get(RowMergingAdapterOptionProvider.ROW_TRANSFORM_KEY);
		final byte[] rowTransformBytes = ByteArrayUtils.byteArrayFromString(rowTransformStr);
		rowTransform = (RowTransform<Mergeable>) PersistenceUtils.fromBinary(rowTransformBytes);
		rowTransform.initOptions(options);
	}

}
