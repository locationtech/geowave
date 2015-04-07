package mil.nga.giat.geowave.store.data.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.GeometryUtils;

import org.apache.commons.lang3.ArrayUtils;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class contains all of the primitive reader field types supported
 * 
 */
public class BasicReader
{

	public static class ArrayReader<T> implements
			FieldReader<T[]>
	{
		private FieldReader<T> baseReader;

		public ArrayReader(
				FieldReader<T> baseReader ) {
			this.baseReader = baseReader;
		}

		@Override
		public T[] readField(
				byte[] fieldData ) {
			// do my array stuff
			ByteBuffer buf = ByteBuffer.wrap(fieldData);
			Object[] retVal = new Object[buf.getInt()];
			for (int i = 0; i < retVal.length; i++) {
				retVal[i] = baseReader.readField(fieldData);
			}
			return (T[]) retVal;
		}

	}

	public static class BooleanReader implements
			FieldReader<Boolean>
	{

		@SuppressFBWarnings(value = {
			"NP_BOOLEAN_RETURN_NULL"
		}, justification = "matches pattern of other read* methods")
		@Override
		public Boolean readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return fieldData[0] > 0;
		}
	}

	public static class ByteReader implements
			FieldReader<Byte>
	{

		@Override
		public Byte readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return fieldData[0];
		}
	}

	public static class ShortReader implements
			FieldReader<Short>
	{

		@Override
		public Short readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 2)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getShort();
		}

	}

	public static class PrimitiveShortArrayReader implements
			FieldReader<short[]>
	{

		@Override
		public short[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 2)) {
				return null;
			}
			final ShortBuffer buff = ByteBuffer.wrap(
					fieldData).asShortBuffer();
			final short[] result = new short[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	public static class FloatReader implements
			FieldReader<Float>
	{

		@Override
		public Float readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getFloat();
		}

	}

	public static class PrimitiveFloatArrayReader implements
			FieldReader<float[]>
	{

		@Override
		public float[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			final FloatBuffer buff = ByteBuffer.wrap(
					fieldData).asFloatBuffer();
			final float[] result = new float[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	public static class DoubleReader implements
			FieldReader<Double>
	{

		@Override
		public Double readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getDouble();
		}

	}

	public static class PrimitiveDoubleArrayReader implements
			FieldReader<double[]>
	{

		@Override
		public double[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			final DoubleBuffer buff = ByteBuffer.wrap(
					fieldData).asDoubleBuffer();
			final double[] result = new double[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	public static class BigDecimalReader implements
			FieldReader<BigDecimal>
	{

		@Override
		public BigDecimal readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 5)) {
				return null;
			}
			final ByteBuffer bb = ByteBuffer.wrap(fieldData);
			final int scale = bb.getInt();
			final byte[] unscaled = new byte[fieldData.length - 4];
			bb.get(unscaled);
			return new BigDecimal(
					new BigInteger(
							unscaled),
					scale);
		}

	}

	public static class IntReader implements
			FieldReader<Integer>
	{

		@Override
		public Integer readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getInt();
		}

	}

	public static class PrimitiveIntArrayReader implements
			FieldReader<int[]>
	{

		@Override
		public int[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			final IntBuffer buff = ByteBuffer.wrap(
					fieldData).asIntBuffer();
			final int[] result = new int[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	public static class LongReader implements
			FieldReader<Long>
	{

		@Override
		public Long readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getLong();
		}

	}

	public static class PrimitiveLongArrayReader implements
			FieldReader<long[]>
	{

		@Override
		public long[] readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			final LongBuffer buff = ByteBuffer.wrap(
					fieldData).asLongBuffer();
			final long[] result = new long[buff.remaining()];
			buff.get(result);
			return result;
		}
	}

	public static class BigIntegerReader implements
			FieldReader<BigInteger>
	{

		@Override
		public BigInteger readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			return new BigInteger(
					fieldData);
		}

	}

	public static class StringReader implements
			FieldReader<String>
	{

		@Override
		public String readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return StringUtils.stringFromBinary(fieldData);
		}

	}

	public static class GeometryReader implements
			FieldReader<Geometry>
	{

		@Override
		public Geometry readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return GeometryUtils.geometryFromBinary(fieldData);
		}

	}

	public static class DateReader implements
			FieldReader<Date>
	{

		@Override
		public Date readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			return new Date(
					ByteBuffer.wrap(
							fieldData).getLong());
		}
	}

	public static class CalendarReader implements
			FieldReader<Calendar>
	{

		@Override
		public Calendar readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			cal.setTime(new Date(
					ByteBuffer.wrap(
							fieldData).getLong()));
			return cal;
		}

	}

	public static class ByteArrayReader implements
			FieldReader<Byte[]>
	{

		@Override
		public Byte[] readField(
				final byte[] fieldData ) {
			return ArrayUtils.toObject(fieldData);
		}

	}

	public static class PrimitiveByteArrayReader implements
			FieldReader<byte[]>
	{

		@Override
		public byte[] readField(
				final byte[] fieldData ) {
			return Arrays.copyOf(
					fieldData,
					fieldData.length);
		}

	}
}
