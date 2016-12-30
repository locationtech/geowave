package mil.nga.giat.geowave.core.store.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClasspathUtils
{

	private static final Logger LOGGER = LoggerFactory.getLogger(
			ClasspathUtils.class);

	public static String setupPathingJarClassPath(
			final File dir,
			final Class context )
			throws IOException {
		return setupPathingJarClassPath(
				new File(
						dir.getParentFile().getAbsolutePath() + File.separator + "pathing",
						"pathing.jar"),
				null,
				context);
	}

	public static String setupPathingJarClassPath(
			final File jarFile,
			final String mainClass,
			final Class context )
			throws IOException {

		final String classpath = getClasspath(
				context);
		final File jarDir = jarFile.getParentFile();
		if (!jarDir.exists()) {
			try {
				jarDir.mkdirs();
			}
			catch (final Exception e) {
				LOGGER.error(
						"Failed to create pathing jar directory: " + e);
				return null;
			}
		}

		if (jarFile.exists()) {
			try {
				jarFile.delete();
			}
			catch (final Exception e) {
				LOGGER.error(
						"Failed to delete old pathing jar: " + e);
				return null;
			}
		}

		// build jar
		final Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(
				Attributes.Name.MANIFEST_VERSION,
				"1.0");
		manifest.getMainAttributes().put(
				Attributes.Name.CLASS_PATH,
				classpath);
		if (mainClass != null) {
			manifest.getMainAttributes().put(
					Attributes.Name.MAIN_CLASS,
					mainClass);
		}

		try (final JarOutputStream target = new JarOutputStream(
				new FileOutputStream(
						jarFile),
				manifest)) {

			target.close();
		}

		return jarFile.getAbsolutePath();
	}

	private static String getClasspath(
			final Class context )
			throws IOException {

		try {
			final ArrayList<ClassLoader> classloaders = new ArrayList<ClassLoader>();

			ClassLoader cl = context.getClassLoader();

			while (cl != null) {
				classloaders.add(
						cl);
				cl = cl.getParent();
			}

			Collections.reverse(
					classloaders);

			final StringBuilder classpathBuilder = new StringBuilder();

			// assume 0 is the system classloader and skip it
			for (int i = 0; i < classloaders.size(); i++) {
				final ClassLoader classLoader = classloaders.get(
						i);

				if (classLoader instanceof URLClassLoader) {

					for (final URL u : ((URLClassLoader) classLoader).getURLs()) {
						append(
								classpathBuilder,
								u);
					}

				}
				else if (classLoader instanceof VFSClassLoader) {

					final VFSClassLoader vcl = (VFSClassLoader) classLoader;
					for (final FileObject f : vcl.getFileObjects()) {
						append(
								classpathBuilder,
								f.getURL());
					}
				}
				else {
					throw new IllegalArgumentException(
							"Unknown classloader type : " + classLoader.getClass().getName());
				}
			}

			classpathBuilder.deleteCharAt(
					0);
			return classpathBuilder.toString();

		}
		catch (final URISyntaxException e) {
			throw new IOException(
					e);
		}
	}

	private static void append(
			final StringBuilder classpathBuilder,
			final URL url )
			throws URISyntaxException {

		final File file = new File(
				url.toURI());

		// do not include dirs containing hadoop or accumulo site files
		if (!containsSiteFile(
				file)) {
			classpathBuilder.append(
					" ").append(
							file.getAbsolutePath().replace(
									"C:\\",
									"file:/C:/").replace(
											"\\",
											"/"));
			if (file.isDirectory()) {
				classpathBuilder.append(
						"/");
			}
		}
	}

	private static boolean containsSiteFile(
			final File f ) {
		if (f.isDirectory()) {
			final File[] sitefile = f.listFiles(
					new FileFilter() {
						@Override
						public boolean accept(
								final File pathname ) {
							return pathname.getName().endsWith(
									"site.xml");
						}
					});

			return (sitefile != null) && (sitefile.length > 0);
		}
		return false;
	}
}
