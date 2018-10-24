package org.locationtech.geowave.core.store.query.options;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Bytes;

public class CommonQueryOptions implements
		Persistable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CommonQueryOptions.class);

	public static class HintKey<HintValueType> implements
			Persistable
	{
		private Class<HintValueType> cls;
		private Function<byte[], HintValueType> reader;
		private Function<HintValueType, byte[]> writer;

		public HintKey() {}

		public HintKey(
				final Class<HintValueType> cls ) {
			this.cls = cls;
			init(cls);
		}

		private void init(
				final Class<HintValueType> cls ) {
			reader = FieldUtils.getDefaultReaderForClass(cls);
			writer = FieldUtils.getDefaultWriterForClass(cls);
		}

		@Override
		public byte[] toBinary() {
			return StringUtils.stringToBinary(cls.getName());
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			try {
				cls = (Class<HintValueType>) Class.forName(StringUtils.stringFromBinary(bytes));
				init(cls);
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error(
						"Class not found for hint",
						e);
			}
		}
	}

	private Map<HintKey<?>, Object> hints;
	private Integer limit;
	private String[] authorizations;

	public CommonQueryOptions(
			final String... authorizations ) {

		this(
				(Integer) null,
				authorizations);
	}

	public CommonQueryOptions(
			final Integer limit,
			final String... authorizations ) {
		this(
				limit,
				new HashMap<>(),
				authorizations);
	}

	public CommonQueryOptions(
			final Integer limit,
			final Map<HintKey<?>, Object> hints,
			final String... authorizations ) {
		super();
		this.hints = hints;
		this.limit = limit;
		this.authorizations = authorizations;
	}

	public Map<HintKey<?>, Object> getHints() {
		return hints;
	}

	public Integer getLimit() {
		return limit;
	}

	public String[] getAuthorizations() {
		return authorizations;
	}

	@Override
	public byte[] toBinary() {
		Integer limitForBinary;
		if (limit == null) {
			limitForBinary = -1;
		}
		else {
			limitForBinary = limit;
		}
		final byte[][] hintsBinary = new byte[hints == null ? 0 : hints.size()][];
		int hintsLength = 0;
		if (hints != null) {
			int i = 0;
			for (final Entry<HintKey<?>, Object> e : hints.entrySet()) {
				final byte[] keyBinary = e.getKey().toBinary();
				hintsBinary[i] = Bytes.concat(
						ByteBuffer.allocate(
								4).putInt(
								keyBinary.length).array(),
						keyBinary,
						((Function<Object, byte[]>) e.getKey().writer).apply(e.getValue()));
				hintsLength += hintsBinary[i].length;
				i++;
			}
		}
		byte[] authsBinary;
		if ((authorizations == null) || (authorizations.length == 0)) {
			authsBinary = new byte[0];
		}
		else {
			authsBinary = StringUtils.stringsToBinary(authorizations);
		}
		final ByteBuffer buf = ByteBuffer.allocate((hintsBinary.length * 4) + 12 + authsBinary.length + hintsLength);
		buf.putInt(limitForBinary);
		buf.putInt(authsBinary.length);
		buf.put(authsBinary);
		buf.putInt(hintsBinary.length);
		for (final byte[] h : hintsBinary) {
			buf.putInt(h.length);
			buf.put(h);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int limit = buf.getInt();
		if (limit <= 0) {
			this.limit = null;
		}
		else {
			this.limit = limit;
		}
		int authLength = buf.getInt();
		if (authLength > 0) {
			final byte[] authBytes = new byte[authLength];

			buf.get(authBytes);
			authorizations = StringUtils.stringsFromBinary(authBytes);
		}
		else {
			authorizations = new String[0];
		}
		final int hintsLength = hints.size();
		final Map<HintKey<?>, Object> hints = new HashMap<>(
				hintsLength);
		for (int i = 0; i < hintsLength; i++) {
			final int l = buf.getInt();
			final byte[] hBytes = new byte[l];
			buf.get(hBytes);
			final ByteBuffer hBuf = ByteBuffer.wrap(hBytes);
			final byte[] keyBytes = new byte[hBuf.getInt()];
			hBuf.get(keyBytes);
			final HintKey<?> key = new HintKey<>();
			key.fromBinary(keyBytes);
			final byte[] vBytes = new byte[hBytes.length - 4 - keyBytes.length];
			hBuf.get(vBytes);
			hints.put(
					key,
					key.reader.apply(vBytes));
		}
		this.hints = hints;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(authorizations);
		result = prime * result + ((hints == null) ? 0 : hints.hashCode());
		result = prime * result + ((limit == null) ? 0 : limit.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		CommonQueryOptions other = (CommonQueryOptions) obj;
		if (!Arrays.equals(
				authorizations,
				other.authorizations)) return false;
		if (hints == null) {
			if (other.hints != null) return false;
		}
		else if (!hints.equals(other.hints)) return false;
		if (limit == null) {
			if (other.limit != null) return false;
		}
		else if (!limit.equals(other.limit)) return false;
		return true;
	}

}