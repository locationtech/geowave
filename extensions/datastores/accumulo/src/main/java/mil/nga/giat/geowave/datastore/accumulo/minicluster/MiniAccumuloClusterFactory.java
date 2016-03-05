package mil.nga.giat.geowave.datastore.accumulo.minicluster;

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

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.log4j.Logger;

public class MiniAccumuloClusterFactory
{

	private static final Logger logger = Logger.getLogger(MiniAccumuloClusterFactory.class);

	public static MiniAccumuloClusterImpl newAccumuloCluster(
			final MiniAccumuloConfigImpl config,
			final Class context )
			throws IOException {

		final String jarPath = setupPathingJarClassPath(
				config.getDir(),
				context);

		if (jarPath == null) {
			// Jar was not successfully created
			return null;
		}

		config.setClasspathItems(jarPath);

		return new MiniAccumuloClusterImpl(
				config);
	}

	private static String setupPathingJarClassPath(
			final File dir,
			final Class context )
			throws IOException {

		final String classpath = getClasspath(context);

		final File jarDir = new File(
				dir.getParentFile().getAbsolutePath() + File.separator + "pathing");
		if (!jarDir.exists()) {
			try {
				jarDir.mkdirs();
			}
			catch (final Exception e) {
				logger.error("Failed to create pathing jar directory: " + e);
				return null;
			}
		}

		final File jarFile = new File(
				jarDir,
				"pathing.jar");

		if (jarFile.exists()) {
			try {
				jarFile.delete();
			}
			catch (final Exception e) {
				logger.error("Failed to delete old pathing jar: " + e);
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

		final JarOutputStream target = new JarOutputStream(
				new FileOutputStream(
						jarFile),
				manifest);

		target.close();

		return jarFile.getAbsolutePath();
	}

	private static String getClasspath(
			final Class context )
			throws IOException {

		try {
			final ArrayList<ClassLoader> classloaders = new ArrayList<ClassLoader>();

			ClassLoader cl = context.getClassLoader();

			while (cl != null) {
				classloaders.add(cl);
				cl = cl.getParent();
			}

			Collections.reverse(classloaders);

			final StringBuilder classpathBuilder = new StringBuilder();

			// assume 0 is the system classloader and skip it
			for (int i = 0; i < classloaders.size(); i++) {
				final ClassLoader classLoader = classloaders.get(i);

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

			classpathBuilder.deleteCharAt(0);
			return classpathBuilder.toString();

		}
		catch (final URISyntaxException e) {
			throw new IOException(
					e);
		}
	}

	private static boolean containsSiteFile(
			final File f ) {
		if (f.isDirectory()) {
			File[] sitefile = f.listFiles(new FileFilter() {
				@Override
				public boolean accept(
						final File pathname ) {
					return pathname.getName().endsWith(
							"site.xml");
				}
			});

			return sitefile != null && sitefile.length > 0;
		}
		return false;
	}

	private static void append(
			final StringBuilder classpathBuilder,
			final URL url )
			throws URISyntaxException {

		final File file = new File(
				url.toURI());

		// do not include dirs containing hadoop or accumulo site files
		if (!containsSiteFile(file)) {
			classpathBuilder.append(
					" ").append(
					file.getAbsolutePath().replace(
							"C:\\",
							"file:/C:/").replace(
							"\\",
							"/"));
			if (file.isDirectory()) {
				classpathBuilder.append("/");
			}
		}
	}
}
