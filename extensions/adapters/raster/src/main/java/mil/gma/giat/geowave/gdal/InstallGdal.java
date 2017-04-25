package mil.gma.giat.geowave.gdal;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.jcraft.jsch.Logger;

import kafka.utils.Os;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;


public class InstallGdal
{
	private final static Logger LOGGER = LoggerFactory.getLogger(InstallGdal.class);
	
	public static final File TEMP_DIR = new File("./target/temp");
	
	/**
	 * Unzips the contents of a zip file to a target output directory
	 *
	 * @param zipInput
	 *            input zip file
	 * @param outputFolder
	 *            zip file output folder
	 *
	 * @param deleteTargetDir
	 *            delete the destination directory before extracting
	 */
	private static void unZipFile(
			final File zipInput,
			final String outputFolder,
			final boolean deleteTargetDir ) {

		try {
			final File of = new File(
					outputFolder);
			if (!of.exists()) {
				if (!of.mkdirs()) {
					throw new IOException(
							"Could not create temporary directory: " + of.toString());
				}
			}
			else if (deleteTargetDir) {
				FileUtil.fullyDelete(of);
			}
			final ZipFile z = new ZipFile(
					zipInput);
			z.extractAll(outputFolder);
		}
		catch (final ZipException e) {
			LOGGER.warn(
					"Unable to extract test data",
					e);
			Assert.fail("Unable to extract test data: '" + e.getLocalizedMessage() + "'");
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to create temporary directory: " + outputFolder,
					e);
			Assert.fail("Unable to extract test data: '" + e.getLocalizedMessage() + "'");
		}
	}
	
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
					TEMP_DIR,
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
			unZipFile(
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
}
