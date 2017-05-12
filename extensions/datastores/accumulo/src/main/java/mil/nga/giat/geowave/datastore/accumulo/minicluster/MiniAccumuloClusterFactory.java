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
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniAccumuloClusterFactory
{

	private static final Logger LOGGER = LoggerFactory.getLogger(MiniAccumuloClusterFactory.class);

	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";

	protected static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

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

		MiniAccumuloClusterImpl retVal = new GeoWaveMiniAccumuloClusterImpl(
				config);
		if (SystemUtils.IS_OS_WINDOWS && isYarn()) {
			// this must happen after instantiating Mini
			// Accumulo Cluster because it ensures the accumulo
			// directory is empty or it will fail, but must
			// happen before the cluster is started because yarn
			// expects winutils.exe to exist within a bin
			// directory in the mini accumulo cluster directory
			// (mini accumulo cluster will always set this
			// directory as hadoop_home)
			LOGGER.info("Running YARN on windows requires a local installation of Hadoop");
			LOGGER.info("'HADOOP_HOME' must be set and 'PATH' must contain %HADOOP_HOME%/bin");

			final Map<String, String> env = System.getenv();
			// HP Fortify "Path Manipulation" false positive
			// What Fortify considers "user input" comes only
			// from users with OS-level access anyway
			String hadoopHome = System.getProperty("hadoop.home.dir");
			if (hadoopHome == null) {
				hadoopHome = env.get("HADOOP_HOME");
			}
			boolean success = false;
			if (hadoopHome != null) {
				final File hadoopDir = new File(
						hadoopHome);
				if (hadoopDir.exists()) {
					final File binDir = new File(
							config.getDir(),
							"bin");
					if (binDir.mkdir()) {
						FileUtils.copyFile(
								new File(
										hadoopDir + File.separator + "bin",
										HADOOP_WINDOWS_UTIL),
								new File(
										binDir,
										HADOOP_WINDOWS_UTIL));
						success = true;
					}
				}
			}
			if (!success) {
				LOGGER
						.error("'HADOOP_HOME' environment variable is not set or <HADOOP_HOME>/bin/winutils.exe does not exist");

				// return mini accumulo cluster anyways
				return retVal;
			}
		}
		return retVal;
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
				LOGGER.error("Failed to create pathing jar directory: " + e);
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
				LOGGER.error("Failed to delete old pathing jar: " + e);
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
			final File[] sitefile = f.listFiles(new FileFilter() {
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
