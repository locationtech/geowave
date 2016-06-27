package mil.nga.giat.geowave.test;

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

import com.jcraft.jsch.Logger;

import kafka.utils.Os;

public class InstallGdal
{
	private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

	public static void main(
			final String[] args )
			throws IOException {
		final File gdalDir;
		if (args.length > 0) {
			gdalDir = new File(
					args[0]);
		}
		else {
			gdalDir = new File(
					TestUtils.TEMP_DIR,
					"gdal");
		}
		if (!gdalDir.exists() && !gdalDir.mkdirs()) {
			LOGGER.warn("unable to create directory " + gdalDir.getAbsolutePath());
		}
		URL url;
		String file;
		if (Os.isWindows()) {
			file = "gdal-1.9.2-MSVC2010-x64.zip";
			url = new URL(
					"http://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.7/native/gdal/windows/MSVC2010/"
							+ file);
		}
		else {
			file = "gdal192-CentOS5.8-gcc4.1.2-x86_64.tar.gz";
			url = new URL(
					"http://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.7/native/gdal/linux/"
							+ file);
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
			TestUtils.unZipFile(
					downloadFile,
					gdalDir.getAbsolutePath(),
					false);
		}
		else {
			final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
			unarchiver.enableLogging(new ConsoleLogger(
					Logger.WARN,
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
}
