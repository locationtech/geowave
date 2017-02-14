package mil.nga.giat.geowave.adapter.raster.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.slf4j.LoggerFactory;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class ZipUtils
{

	private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ZipUtils.class);

	/**
	 * Unzips the contents of a zip file to a target output directory, deleting
	 * anything that existed beforehand
	 *
	 * @param zipInput
	 *            input zip file
	 * @param outputFolder
	 *            zip file output folder
	 *
	 */
	public static void unZipFile(
			final File zipInput,
			final String outputFolder ) {
		unZipFile(
				zipInput,
				outputFolder,
				true);
	}

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
	public static void unZipFile(
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
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to create temporary directory: " + outputFolder,
					e);
		}
	}
}
