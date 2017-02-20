package mil.nga.giat.geowave.datastore.accumulo.util;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;

public class PersistentDataFormatter implements
		Formatter
{
	private static final Logger LOGGER = LoggerFactory.getLogger(PersistentDataFormatter.class);

	public PersistentDataFormatter() {
		super();
	}

	private Iterator<Entry<Key, Value>> si;
	private boolean doTimestamps;
	private static final ThreadLocal<DateFormat> formatter = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new DefaultDateFormat();
		}

		class DefaultDateFormat extends
				DateFormat
		{
			private static final long serialVersionUID = 1L;

			@Override
			public StringBuffer format(
					final Date date,
					final StringBuffer toAppendTo,
					final FieldPosition fieldPosition ) {
				toAppendTo.append(Long.toString(date.getTime()));
				return toAppendTo;
			}

			@Override
			public Date parse(
					final String source,
					final ParsePosition pos ) {
				return new Date(
						Long.parseLong(source));
			}

		}
	};

	@Override
	public void initialize(
			final Iterable<Entry<Key, Value>> scanner,
			final boolean printTimestamps ) {
		checkState(false);
		si = scanner.iterator();
		doTimestamps = printTimestamps;
	}

	@Override
	public boolean hasNext() {
		checkState(true);
		return si.hasNext();
	}

	@Override
	public String next() {
		DateFormat timestampFormat = null;

		if (doTimestamps) {
			timestampFormat = formatter.get();
		}

		return next(timestampFormat);
	}

	protected String next(
			final DateFormat timestampFormat ) {
		checkState(true);
		return formatEntry(
				si.next(),
				timestampFormat);
	}

	@Override
	public void remove() {
		checkState(true);
		si.remove();
	}

	protected void checkState(
			final boolean expectInitialized ) {
		if (expectInitialized && (si == null)) {
			throw new IllegalStateException(
					"Not initialized");
		}
		if (!expectInitialized && (si != null)) {
			throw new IllegalStateException(
					"Already initialized");
		}
	}

	// this should be replaced with something like Record.toString();
	public String formatEntry(
			final Entry<Key, Value> entry,
			final boolean showTimestamps ) {
		DateFormat timestampFormat = null;

		if (showTimestamps) {
			timestampFormat = formatter.get();
		}

		return formatEntry(
				entry,
				timestampFormat);
	}

	/*
	 * so a new date object doesn't get created for every record in the scan
	 * result
	 */
	private static ThreadLocal<Date> tmpDate = new ThreadLocal<Date>() {
		@Override
		protected Date initialValue() {
			return new Date();
		}
	};

	public String formatEntry(
			final Entry<Key, Value> entry,
			final DateFormat timestampFormat ) {
		final StringBuilder sb = new StringBuilder();
		final StringBuilder sbInsertion = new StringBuilder();

		final Key key = entry.getKey();

		final GeoWaveKey rowId = new GeoWaveKeyImpl(
				key.getRow().copyBytes(),
				0);

		byte[] insertionIdBytes;
		insertionIdBytes = rowId.getSortKey();

		for (final byte b : insertionIdBytes) {
			sbInsertion.append(String.format(
					"%02x",
					b));
		}

		final Text insertionIdText = new Text(
				sbInsertion.toString());
		final Text adapterIdText = new Text(
				StringUtils.stringFromBinary(rowId.getAdapterId()));
		final Text dataIdText = new Text(
				StringUtils.stringFromBinary(rowId.getDataId()));
		final Text duplicatesText = new Text(
				Integer.toString(rowId.getNumberOfDuplicates()));

		// append insertion Id
		appendText(
				sb,
				insertionIdText).append(
				" ");

		// append adapterId
		appendText(
				sb,
				adapterIdText).append(
				" ");

		// append dataId
		appendText(
				sb,
				dataIdText).append(
				" ");

		// append numberOfDuplicates
		appendText(
				sb,
				duplicatesText).append(
				" ");

		// append column family
		appendText(
				sb,
				key.getColumnFamily()).append(
				":");

		// append column qualifier
		appendText(
				sb,
				key.getColumnQualifier()).append(
				" ");

		// append visibility expression
		sb.append(new ColumnVisibility(
				key.getColumnVisibility()));

		// append timestamp
		if (timestampFormat != null) {
			tmpDate.get().setTime(
					entry.getKey().getTimestamp());
			sb.append(
					" ").append(
					timestampFormat.format(tmpDate.get()));
		}

		final Value value = entry.getValue();

		// append value
		if ((value != null) && (value.getSize() > 0)) {
			sb.append("\t");
			appendValue(
					sb,
					value);
		}

		return sb.toString();
	}

	private static StringBuilder appendText(
			final StringBuilder sb,
			final Text t ) {
		appendBytes(
				sb,
				t.getBytes(),
				0,
				t.getLength());
		return sb;
	}

	public void appendValue(
			final StringBuilder sb,
			final Value value ) {
		try {
			final Persistable persistable = PersistenceUtils.fromBinary(
					value.get(),
					Persistable.class);
			sb.append(persistable.toString());
		}
		catch (final Exception ex) {
			LOGGER.info(
					"Exception caught",
					ex);
			appendBytes(
					sb,
					value.get(),
					0,
					value.get().length);
		}

	}

	private static void appendBytes(
			final StringBuilder sb,
			final byte ba[],
			final int offset,
			final int len ) {
		for (int i = 0; i < len; i++) {
			final int c = 0xff & ba[offset + i];
			if (c == '\\') {
				sb.append("\\\\");
			}
			else if ((c >= 32) && (c <= 126)) {
				sb.append((char) c);
			}
			else {
				sb.append(
						"\\x").append(
						String.format(
								"%02X",
								c));
			}
		}
	}

	public Iterator<Entry<Key, Value>> getScannerIterator() {
		return si;
	}

	protected boolean isDoTimestamps() {
		return doTimestamps;
	}

}