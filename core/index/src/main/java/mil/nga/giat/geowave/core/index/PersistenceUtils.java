package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A set of convenience methods for serializing and deserializing persistable
 * objects
 * 
 */
public class PersistenceUtils
{

	public static byte[] toBinary(
			final Collection<? extends Persistable> persistables ) {
		if (persistables.isEmpty()) {
			return new byte[] {};
		}
		int byteCount = 4;

		final List<byte[]> persistableBinaries = new ArrayList<byte[]>();
		for (final Persistable persistable : persistables) {
			final byte[] binary = toBinary(persistable);
			byteCount += (4 + binary.length);
			persistableBinaries.add(binary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteCount);
		buf.putInt(persistables.size());
		for (final byte[] binary : persistableBinaries) {
			buf.putInt(binary.length);
			buf.put(binary);
		}
		return buf.array();
	}

	public static byte[] toBinary(
			final Persistable persistable ) {
		if (persistable == null) {
			return new byte[0];
		}
		// preface the payload with the class name and a length of the class
		// name
		byte[] classIdentifier = null;
		try {
			classIdentifier = ClassCompatabilityFactory.getClassIdentifierFromClassName(persistable
					.getClass()
					.getName());
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		if (classIdentifier != null) {
			final byte[] persistableBinary = persistable.toBinary();
			final ByteBuffer buf = ByteBuffer.allocate(4 + classIdentifier.length + persistableBinary.length);
			buf.putInt(classIdentifier.length);
			buf.put(classIdentifier);
			buf.put(persistableBinary);
			return buf.array();
		}
		return new byte[0];
	}

	public static List<Persistable> fromBinary(
			final byte[] bytes ) {
		final List<Persistable> persistables = new ArrayList<Persistable>();
		if ((bytes == null) || (bytes.length < 4)) {
			// the original binary didn't even contain the size of the
			// array, assume that nothing was persisted
			return persistables;
		}
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int size = buf.getInt();
		for (int i = 0; i < size; i++) {
			final byte[] persistableBinary = new byte[buf.getInt()];
			buf.get(persistableBinary);
			persistables.add(fromBinary(
					persistableBinary,
					Persistable.class));
		}
		return persistables;
	}

	public static <T extends Persistable> T fromBinary(
			final byte[] bytes,
			final Class<T> expectedType ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int classNameLength = buf.getInt();

		final byte[] classNameBinary = new byte[classNameLength];
		final byte[] persistableBinary = new byte[bytes.length - classNameLength - 4];
		buf.get(classNameBinary);

		final String className = ClassCompatabilityFactory.getClassNameFromClassIdentifier(classNameBinary);
		final String compatibleClassName = ClassCompatabilityFactory.lookupCompatibleClassName(
				className,
				expectedType.getName());

		final T retVal = classFactory(
				compatibleClassName,
				expectedType);
		if (retVal != null) {
			buf.get(persistableBinary);
			retVal.fromBinary(persistableBinary);
		}
		return (T) retVal;
	}

	public static <T> T classFactory(
			final String className,
			final Class<T> expectedType ) {
		T persistable = PersistableFactory.getPersistable(
				className,
				expectedType);
		if (persistable != null) {
			return (T) persistable;
		}
		return null;
	}
}
