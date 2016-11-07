package mil.nga.giat.geowave.core.store.index.text;

import java.nio.ByteBuffer;
import java.util.Locale;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class TextRangeFilter implements
		DistributableQueryFilter
{
	protected ByteArrayId fieldId;
	protected boolean caseSensitive;
	protected String start;
	protected String end;

	protected TextRangeFilter() {
		super();
	}

	public TextRangeFilter(
			final ByteArrayId fieldId,
			final boolean caseSensitive,
			final String start,
			final String end ) {
		super();
		this.fieldId = fieldId;
		this.caseSensitive = caseSensitive;
		this.start = start;
		this.end = end;
	}

	public ByteArrayId getFieldId() {
		return fieldId;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public String getStart() {
		return start;
	}

	public String getEnd() {
		return end;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArrayId stringBytes = (ByteArrayId) persistenceEncoding.getCommonData().getValue(
				fieldId);
		if (stringBytes != null) {
			String value = stringBytes.getString();
			value = caseSensitive ? value : value.toLowerCase(Locale.ENGLISH);
			final int toStart = value.compareTo(start);
			final int toEnd = value.compareTo(end);
			return ((toStart >= 0) && (toEnd <= 0));
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final byte[] startBytes = StringUtils.stringToBinary(start);
		final byte[] endBytes = StringUtils.stringToBinary(end);
		final ByteBuffer bb = ByteBuffer.allocate(4 + 4 + 4 + fieldId.getBytes().length + 4 + startBytes.length
				+ endBytes.length);
		bb.putInt(fieldId.getBytes().length);
		bb.putInt(startBytes.length);
		bb.putInt(endBytes.length);
		bb.put(fieldId.getBytes());
		final int caseSensitiveInt = (caseSensitive) ? 1 : 0;
		bb.putInt(caseSensitiveInt);
		bb.put(startBytes);
		bb.put(endBytes);
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final int fieldIdBytesLength = bb.getInt();
		final int startBytesLength = bb.getInt();
		final int endBytesLength = bb.getInt();
		final byte[] fieldIdBytes = new byte[fieldIdBytesLength];
		bb.get(fieldIdBytes);
		fieldId = new ByteArrayId(
				fieldIdBytes);
		caseSensitive = (bb.getInt() == 1) ? true : false;
		final byte[] startBytes = new byte[startBytesLength];
		final byte[] endBytes = new byte[endBytesLength];
		bb.get(startBytes);
		bb.get(endBytes);
		start = StringUtils.stringFromBinary(startBytes);
		end = StringUtils.stringFromBinary(endBytes);
	}

}
