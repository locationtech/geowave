package mil.nga.giat.geowave.core.store.index.text;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

// TODO: What if expression has characters [,],(,).  Perhaps it is better to simplify
public class LikeFilter implements
		DistributableQueryFilter
{
	protected String expression;
	protected ByteArrayId fieldId;
	protected Pattern regex;
	protected boolean caseSensitive;

	protected LikeFilter() {
		super();
	}

	public LikeFilter(
			final String expression,
			final ByteArrayId fieldId,
			final Pattern regex,
			final boolean caseSensitive ) {
		super();
		this.expression = expression;
		this.fieldId = fieldId;
		this.regex = regex;
		this.caseSensitive = caseSensitive;
	}

	public String getExpression() {
		return expression;
	}

	public ByteArrayId getFieldId() {
		return fieldId;
	}

	public Pattern getRegex() {
		return regex;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArrayId stringBytes = (ByteArrayId) persistenceEncoding.getCommonData().getValue(
				fieldId);
		if (stringBytes != null) {
			final Matcher matcher = regex.matcher(stringBytes.getString());
			return matcher.matches();
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final byte[] expressionBytes = StringUtils.stringToBinary(expression);
		final ByteBuffer bb = ByteBuffer.allocate(4 + expressionBytes.length + 4 + fieldId.getBytes().length + 4);
		bb.putInt(expressionBytes.length);
		bb.put(expressionBytes);
		bb.putInt(fieldId.getBytes().length);
		bb.put(fieldId.getBytes());
		final int caseSensitiveVal = caseSensitive ? 1 : 0;
		bb.putInt(caseSensitiveVal);
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] expressionBytes = new byte[bb.getInt()];
		bb.get(expressionBytes);
		final byte[] fieldIdBytes = new byte[bb.getInt()];
		bb.get(fieldIdBytes);
		expression = StringUtils.stringFromBinary(expressionBytes);
		fieldId = new ByteArrayId(
				fieldIdBytes);
		caseSensitive = (bb.getInt() == 1) ? true : false;
		regex = Pattern.compile(
				expression.replaceAll(
						"%",
						".*"),
				caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
	}

}
