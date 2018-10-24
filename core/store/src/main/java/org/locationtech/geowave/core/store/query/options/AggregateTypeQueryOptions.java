package org.locationtech.geowave.core.store.query.options;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Aggregation;

public class AggregateTypeQueryOptions<P extends Persistable, R, T> implements
		DataTypeQueryOptions<R>
{
	private String[] typeNames;
	private Aggregation<P, R, T> aggregation;

	public AggregateTypeQueryOptions() {}

	public AggregateTypeQueryOptions(
			final Aggregation<P, R, T> aggregation,
			final String... typeNames ) {
		this.typeNames = typeNames;
		this.aggregation = aggregation;
	}

	@Override
	public String[] getTypeNames() {
		return typeNames;
	}

	public Aggregation<P, R, T> getAggregation() {
		return aggregation;
	}

	@Override
	public byte[] toBinary() {
		byte[] typeNamesBinary, aggregationBinary;
		if ((typeNames != null) && (typeNames.length > 0)) {
			typeNamesBinary = StringUtils.stringsToBinary(typeNames);
		}
		else {
			typeNamesBinary = new byte[0];
		}
		if (aggregation != null) {
			aggregationBinary = PersistenceUtils.toBinary(aggregation);
		}
		else {
			aggregationBinary = new byte[0];
		}
		final ByteBuffer buf = ByteBuffer.allocate(4 + aggregationBinary.length + typeNamesBinary.length);
		buf.putInt(typeNamesBinary.length);
		buf.put(typeNamesBinary);
		buf.put(aggregationBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] typeNamesBytes = new byte[buf.getInt()];
		if (typeNamesBytes.length == 0) {
			typeNames = new String[0];
		}
		else {
			buf.get(typeNamesBytes);
			typeNames = StringUtils.stringsFromBinary(typeNamesBytes);
		}
		final byte[] aggregationBytes = new byte[bytes.length - 4 - typeNamesBytes.length];
		if (aggregationBytes.length == 0) {
			aggregation = null;
		}
		else {
			buf.get(aggregationBytes);
			aggregation = (Aggregation<P, R, T>) PersistenceUtils.fromBinary(aggregationBytes);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((aggregation == null) ? 0 : aggregation.hashCode());
		result = prime * result + Arrays.hashCode(typeNames);
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		AggregateTypeQueryOptions other = (AggregateTypeQueryOptions) obj;
		if (aggregation == null) {
			if (other.aggregation != null) return false;
		}
		else if (!aggregation.equals(other.aggregation)) return false;
		if (!Arrays.equals(
				typeNames,
				other.typeNames)) return false;
		return true;
	}
}
