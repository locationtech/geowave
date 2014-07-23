package mil.nga.giat.geowave.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeSet;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.DataWriter;
import mil.nga.giat.geowave.store.data.PersistentDataset;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A set of convenience methods for common operations on Accumulo within
 * GeoWave, such as conversions between GeoWave objects and corresponding
 * Accumulo objects.
 * 
 */
public class AccumuloUtils
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloUtils.class);
	public final static String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	public static Range byteArrayRangeToAccumuloRange(
			final ByteArrayRange byteArrayRange ) {
		final Text start = new Text(
				byteArrayRange.getStart().getBytes());
		final Text end = new Text(
				byteArrayRange.getEnd().getBytes());
		if (start.compareTo(end) > 0) {
			return null;
		}
		return new Range(
				new Text(
						byteArrayRange.getStart().getBytes()),
				true,
				Range.followingPrefix(new Text(
						byteArrayRange.getEnd().getBytes())),
				false);
	}

	public static TreeSet<Range> byteArrayRangesToAccumuloRanges(
			final List<ByteArrayRange> byteArrayRanges ) {
		if (byteArrayRanges == null) {
			final TreeSet<Range> range = new TreeSet<Range>();
			range.add(new Range());
			return range;
		}
		final TreeSet<Range> accumuloRanges = new TreeSet<Range>();
		for (final ByteArrayRange byteArrayRange : byteArrayRanges) {
			final Range range = byteArrayRangeToAccumuloRange(byteArrayRange);
			if (range == null) {
				continue;
			}
			accumuloRanges.add(range);
		}
		return accumuloRanges;
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_" + unqualifiedTableName;
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final DataAdapter<?> adapter,
			final Index index ) {
		return decodeRow(
				key,
				value,
				adapter,
				null,
				null,
				index);
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final DataAdapter<?> adapter,
			final QueryFilter clientFilter,
			final Index index ) {
		return decodeRow(
				key,
				value,
				adapter,
				null,
				clientFilter,
				index);
	}

	protected static <T> T decodeRow(
			final Key k,
			final Value v,
			DataAdapter<T> adapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index ) {
		if ((adapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		SortedMap<Key, Value> rowMapping;
		try {
			rowMapping = WholeRowIterator.decodeRow(
					k,
					v);
		}
		catch (final IOException e) {
			LOGGER.error("Could not decode row from iterator. Ensure whole row iterators are being used.");
			return null;
		}
		// build a persistence encoding object first, pass it through the
		// client filters and if its accepted, use the data adapter to
		// decode the persistence model into the native data type
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		// for now we are assuming all entries in a row are of the same type
		// and use the same adapter
		boolean adapterMatchVerified;
		ByteArrayId adapterId;
		if (adapter != null) {
			adapterId = adapter.getAdapterId();
			adapterMatchVerified = false;
		}
		else {
			adapterMatchVerified = true;
			adapterId = null;
		}
		for (final Entry<Key, Value> entry : rowMapping.entrySet()) {
			// the column family is the data element's type ID
			if (adapterId == null) {
				adapterId = new ByteArrayId(
						entry.getKey().getColumnFamilyData().getBackingArray());
			}

			if (adapter == null) {
				adapter = (DataAdapter<T>) adapterStore.getAdapter(adapterId);
				if (adapter == null) {
					LOGGER.error("DataAdapter does not exist");
					return null;
				}
			}
			if (!adapterMatchVerified) {
				if (!adapterId.equals(adapter.getAdapterId())) {
					return null;
				}
				adapterMatchVerified = true;
			}
			final ByteArrayId fieldId = new ByteArrayId(
					entry.getKey().getColumnQualifierData().getBackingArray());
			// first check if this field is part of the index model
			final FieldReader<? extends CommonIndexValue> indexFieldReader = index.getIndexModel().getReader(
					fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(entry.getValue().get());
				indexValue.setVisibility(entry.getKey().getColumnVisibilityData().getBackingArray());
				indexData.addValue(new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue));
			}
			else {
				// next check if this field is part of the adapter's
				// extended data model
				final FieldReader<?> extFieldReader = adapter.getReader(fieldId);
				if (extFieldReader == null) {
					// if it still isn't resolved, log an error, and
					// continue
					LOGGER.error("field reader not found for data entry, the value will be ignored");
					continue;
				}
				final Object value = extFieldReader.readField(entry.getValue().get());
				extendedData.addValue(new PersistentValue<Object>(
						fieldId,
						value));
			}
		}
		final ByteSequence rowData = k.getRowData();
		final AccumuloRowId rowId = new AccumuloRowId(
				rowData.getBackingArray());
		final AdapterPersistenceEncoding encodedRow = new AdapterPersistenceEncoding(
				adapterId,
				new ByteArrayId(
						rowId.getDataId()),
				rowId.getNumberOfDuplicates(),
				indexData,
				extendedData);
		if ((clientFilter == null) || clientFilter.accept(encodedRow)) {
			return adapter.decode(
					encodedRow,
					index.getIndexModel());
		}
		return null;
	}

	public static <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final Writer writer ) {
		final List<Mutation> mutations = AccumuloUtils.entryToMutation(
				writableAdapter,
				index,
				entry);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
		for (final Mutation m : mutations) {
			rowIds.add(new ByteArrayId(
					m.getRow()));
		}

		writer.write(mutations);
		return rowIds;
	}

	public static <T> void writeAltIndex(
			final WritableDataAdapter<T> writableAdapter,
			final List<ByteArrayId> rowIds,
			final T entry,
			final Writer writer ) {

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
		final byte[] dataId = writableAdapter.getDataId(
				entry).getBytes();

		final List<Mutation> mutations = new ArrayList<Mutation>();

		for (final ByteArrayId rowId : rowIds) {

			final Mutation mutation = new Mutation(
					new Text(
							dataId));
			mutation.put(
					new Text(
							adapterId),
					new Text(
							rowId.getBytes()),
					new Value(
							"".getBytes()));

			mutations.add(mutation);
		}
		writer.write(mutations);
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static <T> List<Mutation> entryToMutation(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry ) {
		final CommonIndexModel indexModel = index.getIndexModel();
		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final MultiDimensionalNumericData indexNumericData = encodedData.getNumericData(indexModel.getDimensions());
		final List<ByteArrayId> rowIds = index.getIndexStrategy().getInsertionIds(
				indexNumericData);
		final List<Mutation> mutations = new ArrayList<Mutation>();

		final PersistentDataset extendedData = encodedData.getAdapterExtendedData();
		final PersistentDataset indexedData = encodedData.getCommonData();
		final List<PersistentValue> extendedValues = extendedData.getValues();
		final List<PersistentValue> commonValues = indexedData.getValues();

		final int numberOfDuplicates = rowIds.size() - 1;
		final byte[] adapterId = dataWriter.getAdapterId().getBytes();
		final byte[] dataId = dataWriter.getDataId(
				entry).getBytes();
		for (final ByteArrayId rowId : rowIds) {
			final byte[] indexId = rowId.getBytes();
			// because the combination of the adapter ID and data ID gaurantees
			// uniqueness, we combine them in the row ID to disambiguate index
			// values that are the same, also adding enough length values to be
			// able to read the row ID again, we lastly add a number of
			// duplicates which can be useful as metadata in our de-duplication
			// step
			final Mutation mutation = new Mutation(
					new Text(
							new AccumuloRowId(
									indexId,
									dataId,
									adapterId,
									numberOfDuplicates).getRowId()));
			for (final PersistentValue fieldValue : commonValues) {
				addToMutation(
						indexModel,
						fieldValue,
						mutation,
						adapterId,
						entry);
			}
			for (final PersistentValue fieldValue : extendedValues) {
				if (fieldValue.getValue() != null) {
					addToMutation(
							dataWriter,
							fieldValue,
							mutation,
							adapterId,
							entry);
				}
			}
			mutations.add(mutation);
		}
		return mutations;
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static <T> void addToMutation(
			final DataWriter dataWriter,
			final PersistentValue fieldValue,
			final Mutation mutation,
			final byte[] adapterId,
			final T entry ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldValue.getId());
		if (fieldWriter != null) {
			final Object value = fieldValue.getValue();
			mutation.put(
					new Text(
							adapterId),
					new Text(
							fieldValue.getId().getBytes()),
					new ColumnVisibility(
							fieldWriter.getVisibility(
									entry,
									fieldValue.getId(),
									value)),
					new Value(
							fieldWriter.writeField(value)));
		}
		else if (fieldValue.getValue() != null) {
			LOGGER.warn("Data writer of class " + dataWriter.getClass() + " does not support field for " + fieldValue.getValue());
		}
	}
}
