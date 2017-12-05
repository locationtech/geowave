package mil.nga.giat.geowave.core.ingest.spark;

import java.net.URL;
import java.util.Locale;
import java.util.regex.Pattern;

import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;

public class PluginVisitor
{
	protected final Pattern pattern;
	protected final String typeName;
	protected final LocalFileIngestPlugin<?> localPlugin;

	public PluginVisitor(
			final LocalFileIngestPlugin<?> localPlugin,
			final String typeName,
			final String[] userExtensions ) {
		final String[] combinedExtensions = ArrayUtils.addAll(
				localPlugin.getFileExtensionFilters(),
				userExtensions);
		if ((combinedExtensions != null) && (combinedExtensions.length > 0)) {
			final String[] lowerCaseExtensions = new String[combinedExtensions.length];
			for (int i = 0; i < combinedExtensions.length; i++) {
				lowerCaseExtensions[i] = combinedExtensions[i].toLowerCase(Locale.ENGLISH);
			}
			final String extStr = String.format(
					"([^\\s]+(\\.(?i)(%s))$)",
					StringUtils.join(
							lowerCaseExtensions,
							"|"));
			pattern = Pattern.compile(extStr);
		}
		else {
			pattern = null;
		}
		this.localPlugin = localPlugin;
		this.typeName = typeName;
	}

	public boolean supportsFile(
			final URL file ) {
		if ((pattern != null) && !pattern.matcher(
				file.getFile().toLowerCase(
						Locale.ENGLISH)).matches()) {
			return false;
		}
		else if (!localPlugin.supportsFile(file)) {
			return false;
		}
		return true;
	}
}
