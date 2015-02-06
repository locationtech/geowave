package mil.nga.giat.geowave.store.data.field;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.field.BasicWriter.CalendarWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.DateWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.DoubleWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.FloatWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.GeometryWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.IntWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.LongWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.ShortWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.StringWriter;
import mil.nga.giat.geowave.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.store.dimension.Time;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class contains all of the object array writer field types supported
 *
 */
public class ArrayWriter<RowType, FieldType> implements
		FieldWriter<RowType, FieldType[]>
{
	public static enum Encoding {
		FIXED_SIZE_ENCODING(
				(byte) 0),
		VARIABLE_SIZE_ENCODING(
				(byte) 1);

		private final byte encoding;

		Encoding(
				final byte encoding ) {
			this.encoding = encoding;
		}

		public byte getByteEncoding() {
			return encoding;
		}
	}

	private static final Set<Class<?>> FIXED_TYPES = new HashSet<Class<?>>();
	private static final Set<Class<?>> VARIABLE_TYPES = new HashSet<Class<?>>();

	static {
		VARIABLE_TYPES.add(GeometryWrapper[].class);
		VARIABLE_TYPES.add(String[].class);
		VARIABLE_TYPES.add(Geometry[].class);

		FIXED_TYPES.add(Time[].class);
		FIXED_TYPES.add(Short[].class);
		FIXED_TYPES.add(Float[].class);
		FIXED_TYPES.add(Double[].class);
		FIXED_TYPES.add(Integer[].class);
		FIXED_TYPES.add(Long[].class);
		FIXED_TYPES.add(Date[].class);
		FIXED_TYPES.add(Calendar[].class);
	}

	private FieldVisibilityHandler<RowType, Object> visibilityHandler;
	private FieldWriter<RowType, FieldType> writer;

	public ArrayWriter(
			final FieldWriter<RowType, FieldType> writer ) {
		this(
				writer,
				null);
	}

	public ArrayWriter(
			final FieldWriter<RowType, FieldType> writer,
			final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
		this.writer = writer;
		this.visibilityHandler = visibilityHandler;
	}

	@Override
	public byte[] writeField(
			final FieldType[] fieldValue ) {

		final Class<?> clazz = fieldValue.getClass();

		if (VARIABLE_TYPES.contains(clazz)) {
			return writeVariableSizeField(fieldValue);
		}
		else if (FIXED_TYPES.contains(clazz)) {
			return writeFixedSizeField(fieldValue);
		}

		// class type not supported!
		// to be safe, treat as variable size
		return writeVariableSizeField(fieldValue);
	}

	protected byte[] writeFixedSizeField(
			final FieldType[] fieldValue ) {

		final byte[][] byteData = getBytes(fieldValue);

		final ByteBuffer buf = ByteBuffer.allocate(5 + (int) Math.ceil(fieldValue.length / 8.0) + getLength(byteData));

		// this is a header value to indicate how data should be read/written
		buf.put(Encoding.FIXED_SIZE_ENCODING.getByteEncoding());

		int bytesPerEntry = 0;
		for (final byte[] bytes : byteData) {
			if (bytes.length > 0) {
				bytesPerEntry = bytes.length;
			}
		}

		// this is a header value to indicate the size of each entry
		buf.putInt(bytesPerEntry);

		for (int i = 0; i < fieldValue.length; i += 8) {

			int header = 255;

			final int headerIdx = buf.position();
			buf.position(headerIdx + 1);

			for (int j = 0; ((i + j) < fieldValue.length) && (j < 8); j++) {
				final int mask = ~((int) Math.pow(
						2.0,
						j));
				if (fieldValue[i + j] == null) {
					header = header & mask;
				}
				else {
					buf.put(byteData[i + j]);
				}
			}

			buf.put(
					headerIdx,
					(byte) header);
		}

		return buf.array();
	}

	protected byte[] writeVariableSizeField(
			final FieldType[] fieldValue ) {
		final byte[][] bytes = getBytes(fieldValue);
		final ByteBuffer buf = ByteBuffer.allocate(1 + (4 * fieldValue.length) + getLength(bytes));

		// this is a header value to indicate how data should be read/written
		buf.put(Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding());

		for (final byte[] entry : bytes) {
			buf.putInt(entry.length);
			if (entry.length > 0) {
				buf.put(entry);
			}
		}

		return buf.array();
	}

	@Override
	public byte[] getVisibility(
			final RowType rowValue,
			final ByteArrayId fieldId,
			final FieldType[] fieldValue ) {
		if (visibilityHandler != null) {
			return visibilityHandler.getVisibility(
					rowValue,
					fieldId,
					fieldValue);
		}
		return new byte[] {};
	}

	private byte[][] getBytes(
			final FieldType[] fieldData ) {
		final byte[][] bytes = new byte[fieldData.length][];
		for (int i = 0; i < fieldData.length; i++) {
			if (fieldData[i] == null) {
				bytes[i] = new byte[] {};
			}
			else {
				bytes[i] = writer.writeField(fieldData[i]);
			}
		}
		return bytes;
	}

	private int getLength(
			final byte[][] bytes ) {
		int length = 0;
		for (final byte[] entry : bytes) {
			length += entry.length;
		}
		return length;
	}

	public static class FixedSizeObjectArrayWriter<RowType, FieldType> extends
			ArrayWriter<RowType, FieldType>
	{
		public FixedSizeObjectArrayWriter(
				final FieldWriter<RowType, FieldType> writer ) {
			super(
					writer);
		}

		public FixedSizeObjectArrayWriter(
				final FieldWriter<RowType, FieldType> writer,
				final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
			super(
					writer,
					visibilityHandler);
		}

		@Override
		public byte[] writeField(
				final FieldType[] fieldValue ) {
			return super.writeFixedSizeField(fieldValue);
		}
	}

	public static class ShortArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Short>
	{
		public ShortArrayWriter() {
			super(
					new ShortWriter());
		}
	}

	public static class FloatArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Float>
	{
		public FloatArrayWriter() {
			super(
					new FloatWriter());
		}
	}

	public static class DoubleArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Double>
	{
		public DoubleArrayWriter() {
			super(
					new DoubleWriter());
		}
	}

	public static class IntArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Integer>
	{
		public IntArrayWriter() {
			super(
					new IntWriter());
		}
	}

	public static class LongArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Long>
	{
		public LongArrayWriter() {
			super(
					new LongWriter());
		}
	}

	public static class DateArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Date>
	{
		public DateArrayWriter() {
			super(
					new DateWriter());
		}
	}

	public static class CalendarArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Calendar>
	{
		public CalendarArrayWriter() {
			super(
					new CalendarWriter());
		}
	}

	public static class VariableSizeObjectArrayWriter<RowType, FieldType> extends
			ArrayWriter<RowType, FieldType>
	{
		public VariableSizeObjectArrayWriter(
				final FieldWriter<RowType, FieldType> writer ) {
			super(
					writer);
		}

		public VariableSizeObjectArrayWriter(
				final FieldWriter<RowType, FieldType> writer,
				final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
			super(
					writer,
					visibilityHandler);
		}

		@Override
		public byte[] writeField(
				final FieldType[] fieldValue ) {
			return super.writeVariableSizeField(fieldValue);
		}
	}

	public static class StringArrayWriter extends
			VariableSizeObjectArrayWriter<Object, String>
	{
		public StringArrayWriter() {
			super(
					new StringWriter());
		}
	}

	public static class GeometryArrayWriter extends
			VariableSizeObjectArrayWriter<Object, Geometry>
	{
		public GeometryArrayWriter() {
			super(
					new GeometryWriter());
		}
	}
}
