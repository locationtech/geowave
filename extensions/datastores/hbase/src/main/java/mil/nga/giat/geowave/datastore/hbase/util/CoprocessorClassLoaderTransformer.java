package mil.nga.giat.geowave.datastore.hbase.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.spi.ClassLoaderTransformerSpi;

public class CoprocessorClassLoaderTransformer implements
		ClassLoaderTransformerSpi
{

	private final static Logger LOGGER = LoggerFactory.getLogger(CoprocessorClassLoaderTransformer.class);

	@Override
	public ClassLoader transform(
			final ClassLoader classLoader ) {
		if (classLoader instanceof CoprocessorClassLoader) {
			final ClassLoader cl = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
				@Override
				public ClassLoader run() {
					try {
						final Field field = classLoader.getClass().getDeclaredField(
								"CLASS_PREFIX_EXEMPTIONS");
						field.setAccessible(true);
						final Field modifiersField = Field.class.getDeclaredField("modifiers");
						modifiersField.setAccessible(true);
						modifiersField.setInt(
								field,
								field.getModifiers() & ~Modifier.FINAL);
						final Object fieldValue = field.get(classLoader);
						if (fieldValue instanceof String[]) {
							final List<String> strList = new ArrayList<>(
									Arrays.asList((String[]) fieldValue));
							if (strList.remove("javax.")) {
								// we want to at least exclude javax.measure and
								// javax.media from this exemption list so we do
								// so by removing javax. and then adding
								// prefixes more explicitly that are provided
								// within the jdk
								strList.add("javax.a");
								strList.add("javax.imageio");
								strList.add("javax.jws");
								strList.add("javax.lang");
								strList.add("javax.management");
								strList.add("javax.n");
								strList.add("javax.rmi");
								strList.add("javax.print");
								strList.add("javax.s");
								strList.add("javax.t");
								strList.add("javax.x");
								field.set(
										classLoader,
										strList.toArray(new String[strList.size()]));
								return classLoader;
							}
						}
					}
					catch (final Exception e) {
						LOGGER.warn(
								"Unable to modify classloader",
								e);
					}

					return null;
				}
			});
			return cl;
		}
		return null;
	}

}
