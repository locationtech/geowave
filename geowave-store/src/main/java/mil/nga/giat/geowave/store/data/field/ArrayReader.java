package mil.nga.giat.geowave.store.data.field;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.store.data.field.ArrayWriter.Encoding;
import mil.nga.giat.geowave.store.data.field.BasicReader.CalendarReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.DateReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.DoubleReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.FloatReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.GeometryReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.IntReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.LongReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.ShortReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.StringReader;
import mil.nga.giat.geowave.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.store.dimension.Time;
import mil.nga.giat.geowave.store.filter.GenericTypeResolver;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class contains all of the object array reader field types supported
 *
 */
public class ArrayReader<FieldType> implements
		FieldReader<FieldType[]>
{

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

	private final FieldReader<FieldType> reader;

	public ArrayReader(
			final FieldReader<FieldType> reader ) {
		this.reader = reader;
	}

	@Override
	public FieldType[] readField(
			final byte[] fieldData ) {

		final byte encoding = fieldData[0];

		// try to read the encoding first
		if (encoding == Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
			return readFixedSizeField(fieldData);
		}
		else if (encoding == Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
			return readVariableSizeField(fieldData);
		}

		// class type not supported!
		// to be safe, treat as variable size
		return readVariableSizeField(fieldData);
	}

	@SuppressWarnings("unchecked")
	protected FieldType[] readFixedSizeField(
			final byte[] fieldData ) {
		if (fieldData.length < 1) {
			return null;
		}

		final List<FieldType> result = new ArrayList<FieldType>();

		final ByteBuffer buff = ByteBuffer.wrap(fieldData);

		// this would be bad
		if (buff.get() != Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
			return null;
		}

		final int bytesPerEntry = buff.getInt();

		final byte[] data = new byte[bytesPerEntry];

		while (buff.remaining() > 0) {

			final int header = buff.get();

			for (int i = 0; i < 8; i++) {

				final int mask = (int) Math.pow(
						2.0,
						i);

				if ((header & mask) != 0) {
					if (buff.remaining() > 0) {
						buff.get(data);
						result.add(reader.readField(data));
					}
					else {
						break;
					}
				}
				else {
					result.add(null);
				}
			}
		}
		final FieldType[] resultArray = (FieldType[]) Array.newInstance(
				GenericTypeResolver.resolveTypeArgument(
						reader.getClass(),
						FieldReader.class),
				result.size());
		return result.toArray(resultArray);
	}

	@SuppressWarnings("unchecked")
	protected FieldType[] readVariableSizeField(
			final byte[] fieldData ) {
		if ((fieldData == null) || (fieldData.length < 4)) {
			return null;
		}
		final List<FieldType> result = new ArrayList<FieldType>();

		final ByteBuffer buff = ByteBuffer.wrap(fieldData);

		// this would be bad
		if (buff.get() != Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
			return null;
		}

		while (buff.remaining() >= 4) {
			final int size = buff.getInt();
			if (size > 0) {
				final byte[] bytes = new byte[size];
				buff.get(bytes);
				result.add(reader.readField(bytes));
			}
			else {
				result.add(null);
			}
		}
		final FieldType[] resultArray = (FieldType[]) Array.newInstance(
				GenericTypeResolver.resolveTypeArgument(
						reader.getClass(),
						FieldReader.class),
				result.size());
		return result.toArray(resultArray);
	}

	public static class FixedSizeObjectArrayReader<FieldType> extends
			ArrayReader<FieldType>
	{
		public FixedSizeObjectArrayReader(
				final FieldReader<FieldType> reader ) {
			super(
					reader);
		}

		@Override
		public FieldType[] readField(
				final byte[] fieldData ) {
			return readFixedSizeField(fieldData);
		}
	}

	public static class ShortArrayReader extends
			FixedSizeObjectArrayReader<Short>
	{
		public ShortArrayReader() {
			super(
					new ShortReader());
		}
	}

	public static class FloatArrayReader extends
			FixedSizeObjectArrayReader<Float>
	{
		public FloatArrayReader() {
			super(
					new FloatReader());
		}
	}

	public static class DoubleArrayReader extends
			FixedSizeObjectArrayReader<Double>
	{
		public DoubleArrayReader() {
			super(
					new DoubleReader());
		}
	}

	public static class IntArrayReader extends
			FixedSizeObjectArrayReader<Integer>
	{
		public IntArrayReader() {
			super(
					new IntReader());
		}
	}

	public static class LongArrayReader extends
			FixedSizeObjectArrayReader<Long>
	{
		public LongArrayReader() {
			super(
					new LongReader());
		}
	}

	public static class DateArrayReader extends
			FixedSizeObjectArrayReader<Date>
	{
		public DateArrayReader() {
			super(
					new DateReader());
		}
	}

	public static class CalendarArrayReader extends
			FixedSizeObjectArrayReader<Calendar>
	{
		public CalendarArrayReader() {
			super(
					new CalendarReader());
		}
	}

	public static class VariableSizeObjectArrayReader<FieldType> extends
			ArrayReader<FieldType>
	{
		public VariableSizeObjectArrayReader(
				final FieldReader<FieldType> reader ) {
			super(
					reader);
		}

		@Override
		public FieldType[] readField(
				final byte[] fieldData ) {
			return readVariableSizeField(fieldData);
		}
	}

	public static class StringArrayReader extends
			VariableSizeObjectArrayReader<String>
	{
		public StringArrayReader() {
			super(
					new StringReader());
		}
	}

	public static class GeometryArrayReader extends
			VariableSizeObjectArrayReader<Geometry>
	{
		public GeometryArrayReader() {
			super(
					new GeometryReader());
		}
	}
}
