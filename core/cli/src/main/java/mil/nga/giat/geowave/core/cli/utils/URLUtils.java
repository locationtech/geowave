/**
 * 
 */
package mil.nga.giat.geowave.core.cli.utils;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Utility methods relating to URLs, particularly validation focused
 */
public class URLUtils
{
	private static final String HTTP = "http";
	private static final String HTTPS = "https";
	private static String[] schemes = {
		HTTP,
		HTTPS
	};

	public static String getUrl(
			String url )
			throws URISyntaxException,
			MalformedURLException {
		if (url != null) {
			if (isValidURL(url)) {
				return url;
			}
			boolean valid = false;
			for (String scheme : getSchemes()) {
				if (scheme.toLowerCase().startsWith(
						scheme + "://")) {
					valid = true;
				}
			}
			if (!valid) {
				url = HTTP + "://" + url;
			}
			URI uri = new URI(
					url);
			if (uri.getScheme() == null) {
				uri = new URI(
						HTTP + "://" + url);
			}
			URL targetURL = uri.toURL();
			if (targetURL.getPort() == -1) {
				targetURL = new URL(
						targetURL.getProtocol(),
						targetURL.getHost(),
						targetURL.getDefaultPort(),
						targetURL.getFile());
			}
			if (String.valueOf(
					targetURL.getPort()).endsWith(
					"443")) {
				targetURL = new URL(
						HTTPS,
						targetURL.getHost(),
						targetURL.getPort(),
						targetURL.getFile());
			}
			return targetURL.toString();
		}
		return url;
	}

	/**
	 * Validate a URL to quickly check if it is in proper URL format
	 * 
	 * @param url
	 *            url to validate
	 * @return true if valid, false otherwise
	 */
	private static boolean isValidURL(
			String url ) {
		URL targetURL = null;
		try {
			targetURL = new URL(
					url);
		}
		catch (MalformedURLException e) {
			return false;
		}

		try {
			targetURL.toURI();
		}
		catch (URISyntaxException e) {
			return false;
		}
		return true;
	}

	/**
	 * @return the schemes
	 */
	public static String[] getSchemes() {
		return schemes;
	}

	/**
	 * @param schemes
	 *            the schemes to set
	 */
	public static void setSchemes(
			String[] schemes ) {
		URLUtils.schemes = schemes;
	}
}