package mil.nga.giat.geowave.adapter.raster.plugin.gdal;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;

public class InstallGdal
{
	private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(InstallGdal.class);

	public static final File DEFAULT_TEMP_DIR = new File(
			"./target/temp");
	private static final String GDAL_ENV = "baseGdalDownload";
	private static final String DEFAULT_BASE = "http://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.7/native/gdal";

	public static void main(
			final String[] args )
			throws IOException {

		File gdalDir = null;
		if (args.length > 0) {
			gdalDir = new File(
					args[0]);
		}
		else {
			gdalDir = new File(
					DEFAULT_TEMP_DIR,
					"gdal");
		}

		if (gdalDir.exists()) {
			return;
		}

		if (!gdalDir.mkdirs()) {
			LOGGER.warn("unable to create directory " + gdalDir.getAbsolutePath());
		}

		install(gdalDir);
	}

	private static void install(
			File gdalDir )
			throws IOException {
		URL url;
		String file;
		String gdalEnv = System.getProperty(GDAL_ENV);
		if (gdalEnv == null || gdalEnv.trim().isEmpty()) {
			gdalEnv = DEFAULT_BASE;
		}
		if (isWindows()) {
			file = "gdal-1.9.2-MSVC2010-x64.zip";
			url = new URL(
					gdalEnv + "/windows/MSVC2010/" + file);
		}
		else {
			file = "gdal192-CentOS5.8-gcc4.1.2-x86_64.tar.gz";
			url = new URL(
					gdalEnv + "/linux/" + file);
		}
		final File downloadFile = new File(
				gdalDir,
				file);
		if (!downloadFile.exists()) {
			try (FileOutputStream fos = new FileOutputStream(
					downloadFile)) {
				IOUtils.copyLarge(
						url.openStream(),
						fos);
				fos.flush();
			}
		}
		if (file.endsWith("zip")) {
			ZipUtils.unZipFile(
					downloadFile,
					gdalDir.getAbsolutePath(),
					false);
		}
		else {
			final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
			unarchiver.enableLogging(new ConsoleLogger(
					org.codehaus.plexus.logging.Logger.LEVEL_WARN,
					"GDAL Unarchive"));
			unarchiver.setSourceFile(downloadFile);
			unarchiver.setDestDirectory(gdalDir);
			unarchiver.extract();
			// the symbolic links are not working, programmatically re-create
			// them
			final File[] links = gdalDir.listFiles(new FileFilter() {
				@Override
				public boolean accept(
						final File pathname ) {
					return pathname.length() <= 0;
				}
			});
			if (links != null) {
				final File[] actualLibs = gdalDir.listFiles(new FileFilter() {
					@Override
					public boolean accept(
							final File pathname ) {
						return pathname.length() > 0;
					}
				});
				for (final File link : links) {
					// find an actual lib that matches
					for (final File lib : actualLibs) {
						if (lib.getName().startsWith(
								link.getName())) {
							if (link.delete()) {
								Files.createSymbolicLink(
										link.getAbsoluteFile().toPath(),
										lib.getAbsoluteFile().toPath());
							}
							break;
						}
					}
				}
			}
		}
		if (!downloadFile.delete()) {
			LOGGER.warn("cannot delete " + downloadFile.getAbsolutePath());
		}
	}

	private static boolean isWindows() {
		String name = System.getProperty(
				"os.name").toLowerCase();
		return name.startsWith("windows");
	}

}
