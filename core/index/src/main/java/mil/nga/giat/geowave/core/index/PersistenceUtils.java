package mil.nga.giat.geowave.core.index;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of convenience methods for serializing and deserializing persistable
 * objects
 * 
 */
public class PersistenceUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PersistenceUtils.class);

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
		final byte[] className = StringUtils.stringToBinary(persistable.getClass().getName());
		final byte[] persistableBinary = persistable.toBinary();
		final int classNameLength = className.length;
		final ByteBuffer buf = ByteBuffer.allocate(4 + classNameLength + persistableBinary.length);
		buf.putInt(classNameLength);
		buf.put(className);
		buf.put(persistableBinary);
		return buf.array();
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

		final String className = StringUtils.stringFromBinary(classNameBinary);

		final T retVal = classFactory(
				className,
				expectedType);
		if (retVal != null) {
			buf.get(persistableBinary);
			retVal.fromBinary(persistableBinary);
		}
		return retVal;
	}

	@SuppressWarnings("unchecked")
	public static <T> T classFactory(
			final String className,
			final Class<T> expectedType ) {
		Class<?> factoryType = null;

		try {
			factoryType = Class.forName(className);
		}
		catch (final ClassNotFoundException e) {
			LOGGER.warn(
					"error creating class: could not find class ",
					e);
		}

		if (factoryType != null) {
			Object factoryClassInst = null;

			try {
				// use the no arg constructor and make sure its accessible

				// HP Fortify "Access Specifier Manipulation"
				// This method is being modified by trusted code,
				// in a way that is not influenced by user input
				final Constructor<?> noArgConstructor = factoryType.getDeclaredConstructor();
				noArgConstructor.setAccessible(true);
				factoryClassInst = noArgConstructor.newInstance();
			}
			catch (final Exception e) {
				LOGGER.warn(
						"error creating class: could not create class ",
						e);
			}

			if (factoryClassInst != null) {
				if (!expectedType.isAssignableFrom(factoryClassInst.getClass())) {
					LOGGER.warn("error creating class, does not implement expected type");
				}
				else {
					return ((T) factoryClassInst);
				}
			}
		}

		return null;
	}
}
