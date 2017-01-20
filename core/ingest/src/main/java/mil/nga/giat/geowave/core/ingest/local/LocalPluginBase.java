package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;

/**
 * This is a base interface for any plugin that reads files from a local file
 * system. The plugin gets an init call at the start of ingestion with the base
 * directory, and can filter files based on extension or act on a file-by-file
 * basis for anything more complex.
 */
public interface LocalPluginBase
{
	/**
	 * Gets a list of file extensions that this plugin supports. If not
	 * provided, this plugin will accept all file extensions.
	 * 
	 * @return The array of file extensions supported ('.' is unnecessary)
	 */
	public String[] getFileExtensionFilters();

	/**
	 * Initialize the plugin and give it the base directory that is provided by
	 * the user.
	 * 
	 * @param baseDirectory
	 *            The base directory provided as a command-line argument (if the
	 *            argument is a file, the base directory given will be its
	 *            parent directory).
	 */
	public void init(
			File baseDirectory );

	/**
	 * This method will be called for every file that matches the given
	 * extensions. It is an opportunity for the plugin to perform arbitrarily
	 * complex acceptance filtering on a per file basis, but it is important to
	 * understand performance implications if the acceptance test is too
	 * intensive and the directory of files to recurse is large.
	 * 
	 * @param file
	 *            The file to determine if this plugin supports for ingestion
	 * @return Whether the file is supported or not
	 */
	public boolean supportsFile(
			File file );
}
