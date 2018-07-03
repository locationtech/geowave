package mil.nga.giat.geowave.datastore.hbase.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.mapreduce.URLClassloaderUtils;

public class MergingServerOp implements
		HBaseServerOp
{
	public static Object MUTEX = new Object();
	protected Set<GeowaveColumnId> columnFamilyIds = new HashSet<>();
	// protected Set<ByteArrayId> columnFamilyIds = new HashSet<>();
	private static final String OLD_MAX_VERSIONS_KEY = "MAX_VERSIONS";

	protected Mergeable getMergeable(
			final Cell cell,
			final byte[] bytes ) {
		return (Mergeable) URLClassloaderUtils.fromBinary(bytes);
	}

	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return URLClassloaderUtils.toBinary(mergeable);
	}

	@Override
	public boolean nextRow(
			final RowScanner rowScanner )
			throws IOException {
		synchronized (MUTEX) {
			do {
				// a reference variable to all the current cells
				final List<Cell> rowCells = rowScanner.currentCellsInRow();

				if (rowCells.size() > 1) {
					Integer maxVersions = null;
					if (rowScanner.getScan() != null) {
						final Object oldMaxObj = rowScanner.getHints().get(
								OLD_MAX_VERSIONS_KEY);
						if ((oldMaxObj == null) || !(oldMaxObj instanceof Integer)) {
							final byte[] oldMaxVersions = rowScanner.getScan().getAttribute(
									OLD_MAX_VERSIONS_KEY);
							if (oldMaxVersions != null) {
								maxVersions = ByteBuffer.wrap(
										oldMaxVersions).getInt();
								// cache it in a "hints" map to avoid multiple
								// byte buffer allocations
								rowScanner.getHints().put(
										OLD_MAX_VERSIONS_KEY,
										maxVersions);
							}
						}
						else {
							maxVersions = (Integer) oldMaxObj;
						}
					}
					final Iterator<Cell> iter = rowCells.iterator();
					final Map<PartialCellEquality, List<Cell>> merges = new HashMap<>();
					final Map<PartialCellEquality, List<Cell>> nonMerges = new HashMap<>();
					// iterate once to capture individual tags/visibilities
					boolean rebuildList = false;
					while (iter.hasNext()) {
						final Cell cell = iter.next();
						// TODO consider avoiding extra byte array allocations
						final byte[] familyBytes = CellUtil.cloneFamily(cell);
						GeowaveColumnId familyId = null;
						if (columnFamilyIds.iterator().next() instanceof ShortColumnId) {
							familyId = new ShortColumnId(
									ByteArrayUtils.shortFromString(StringUtils.stringFromBinary(familyBytes)));
						}
						else if (columnFamilyIds.iterator().next() instanceof ByteArrayColumnId) {
							familyId = new ByteArrayColumnId(
									new ByteArrayId(
											familyBytes));
						}

						if (columnFamilyIds.contains(familyId)) {
							final PartialCellEquality key = new PartialCellEquality(
									cell,
									includeTags());
							List<Cell> cells = merges.get(key);
							if (cells == null) {
								cells = new ArrayList<>();
								merges.put(
										key,
										cells);
							}
							else {
								// this implies there is more than one cell with
								// the
								// same vis, so merging will need to take place
								rebuildList = true;
							}
							cells.add(cell);
						}
						else {
							// always include tags for non-merge cells so that
							// versioning works as expected
							final PartialCellEquality key = new PartialCellEquality(
									cell,
									true);
							// get max versions and trim these cells to max
							// versions
							// per column family and qualifier, and tags
							List<Cell> cells = nonMerges.get(key);
							if (cells == null) {
								cells = new ArrayList<>();
								nonMerges.put(
										key,
										cells);
							}
							else if ((maxVersions != null) && (cells.size() >= maxVersions)) {
								rebuildList = true;
							}
							cells.add(cell);
						}
					}
					if (rebuildList) {
						rowCells.clear();
						for (final List<Cell> cells : merges.values()) {
							if (cells.size() > 1) {
								rowCells.add(mergeList(cells));
							}
							else if (cells.size() == 1) {
								rowCells.add(cells.get(0));
							}
						}
						for (final List<Cell> cells : nonMerges.values()) {
							if ((maxVersions != null) && (cells.size() > maxVersions)) {
								rowCells.addAll(cells.subList(
										0,
										maxVersions));
							}
							else {
								rowCells.addAll(cells);
							}
						}
						// these have to stay in order and they can get out of
						// order when adding cells from 2 maps
						rowCells.sort(new CellComparator());
					}
				}
			}
			while (!rowScanner.nextCellsInRow().isEmpty());
			return true;
		}
	}

	protected boolean includeTags() {
		return true;
	}

	protected Cell mergeList(
			final List<Cell> cells ) {
		synchronized (MUTEX) {
			Mergeable currentMergeable = null;
			final Cell firstCell = cells.get(0);
			for (final Cell cell : cells) {
				final Mergeable mergeable = getMergeable(
						cell,
						// TODO consider avoiding extra byte array
						// allocations (which would require
						// persistence utils to be able to use
						// bytebuffer instead of byte[])
						CellUtil.cloneValue(cell));
				if (mergeable != null) {
					if (currentMergeable == null) {
						currentMergeable = mergeable;
					}
					else {
						currentMergeable.merge(mergeable);
					}
				}
			}
			final byte[] valueBinary = getBinary(currentMergeable);
			// this is basically a lengthy verbose form of cloning
			// in-place (without allocating new byte arrays) and
			// simply replacing the value with the new mergeable
			// value
			return new KeyValue(
					firstCell.getRowArray(),
					firstCell.getRowOffset(),
					firstCell.getRowLength(),
					firstCell.getFamilyArray(),
					firstCell.getFamilyOffset(),
					firstCell.getFamilyLength(),
					firstCell.getQualifierArray(),
					firstCell.getQualifierOffset(),
					firstCell.getQualifierLength(),
					firstCell.getTimestamp(),
					Type.codeToType(firstCell.getTypeByte()),
					valueBinary,
					0,
					valueBinary.length,
					firstCell.getTagsArray(),
					firstCell.getTagsOffset(),
					firstCell.getTagsLength());
		}
	}

	@Override
	public void init(
			final Map<String, String> options )
			throws IOException {
		final String columnStr = getColumnOptionValue(options);

		if (columnStr.length() == 0) {
			throw new IllegalArgumentException(
					"The column must not be empty");
		}
		columnFamilyIds = Sets.newHashSet(Iterables.transform(
				Splitter.on(
						",").split(
						columnStr),
				new Function<String, GeowaveColumnId>() {

					@Override
					public GeowaveColumnId apply(
							final String input ) {
						return new ByteArrayColumnId(
								new ByteArrayId(
										input));
					}
				}));
	}

	protected String getColumnOptionValue(
			final Map<String, String> options ) {
		// if this is not "row" merging than it is merging stats on the metadata
		// table
		return MetadataType.STATS.name();
	}

	@Override
	public void preScannerOpen(
			final Scan scan ) {
		final int maxVersions = scan.getMaxVersions();
		if ((maxVersions > 0) && (maxVersions < Integer.MAX_VALUE)) {
			scan.setAttribute(
					OLD_MAX_VERSIONS_KEY,
					ByteBuffer.allocate(
							4).putInt(
							maxVersions).array());

		}
		scan.setMaxVersions();
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}
}
