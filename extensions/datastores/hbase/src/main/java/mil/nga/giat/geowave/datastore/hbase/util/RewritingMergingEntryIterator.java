package mil.nga.giat.geowave.datastore.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;

public class RewritingMergingEntryIterator<T> extends
		MergingEntryIterator<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RewritingMergingEntryIterator.class);

	private final HBaseWriter writer;

	public RewritingMergingEntryIterator(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters,
			final HBaseWriter writer ) {
		super(
				adapterStore,
				index,
				scannerIt,
				null,
				null,
				mergingAdapters,
				null,
				null,
				false);
		this.writer = writer;
	}

	@Override
	protected Result mergeResults(
			final RowMergingDataAdapter mergingAdapter,
			final ArrayList<Result> resultsToMerge ) {
		final Result retVal = super.mergeResults(
				mergingAdapter,
				resultsToMerge);

		final List<RowMutations> rowMutationsList = new ArrayList<>();
		final byte[] uniqueRow = HBaseUtils.ensureUniqueId(
				retVal.getRow(),
				true).getBytes();
		final RowMutations putRowMutations = new RowMutations(
				uniqueRow);
		try {
			for (final Cell oldCell : retVal.rawCells()) {
				final Cell newCell = CellUtil.createCell(
						uniqueRow,
						CellUtil.cloneFamily(oldCell),
						CellUtil.cloneQualifier(oldCell),
						oldCell.getTimestamp(),
						oldCell.getTypeByte(),
						CellUtil.cloneValue(oldCell));
				final Put p = new Put(
						uniqueRow);
				p.add(newCell);
				putRowMutations.add(p);
			}

			rowMutationsList.add(putRowMutations);
			for (final Result input : resultsToMerge) {
				final RowMutations deleteRowMutations = new RowMutations(
						input.getRow());
				deleteRowMutations.add(new Delete(
						input.getRow()));
				rowMutationsList.add(deleteRowMutations);
			}

			writer.write(rowMutationsList);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to rewrite row",
					e);
		}
		return retVal;
	}
}
