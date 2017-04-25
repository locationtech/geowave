package mil.nga.giat.geowave.adapter.auth;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * Use the user details to to determine a user's name. Given the user's name,
 * lookup the user credentials in a Json file. The location of the file is
 * provided through the URL (protocol is file).
 * 
 * @author rwgdrummer
 * 
 */
public class JsonFileAuthorizationProvider implements
		AuthorizationSPI
{

	private AuthorizationSet authorizationSet;

	public JsonFileAuthorizationProvider(
			final URL location ) {
		if (location == null) {
			authorizationSet = new AuthorizationSet();
		}
		else {
			String path = location.getPath();
			if (!location.getProtocol().equals(
					"file") || (!new File(
					path).canRead() && !new File(
					"." + path).canRead())) {
				throw new IllegalArgumentException(
						"Cannot find file " + location.toString());
			}
			try {
				if (!new File(
						path).canRead()) {
					path = "." + path;
				}
				parse(new File(
						path));
			}
			catch (final JsonParseException e) {
				throw new IllegalArgumentException(
						"Cannot parse file " + location.toString(),
						e);
			}
			catch (final JsonMappingException e) {
				throw new IllegalArgumentException(
						"Cannot parse file " + location.toString(),
						e);
			}
			catch (final IOException e) {
				throw new IllegalArgumentException(
						"Cannot parse file " + location.toString(),
						e);
			}
		}
	}

	private void parse(
			final File file )
			throws JsonParseException,
			JsonMappingException,
			IOException {
		final ObjectMapper mapper = new ObjectMapper();
		authorizationSet = mapper.readValue(
				file,
				AuthorizationSet.class);
	}

	@Override
	public String[] getAuthorizations() {
		final Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (auth == null) {
			return new String[0];
		}
		final Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
		String userName = principal.toString();
		if (principal instanceof UserDetails) {
			// most likely type of principal
			final UserDetails userDetails = (UserDetails) principal;
			userName = userDetails.getUsername();
		}
		final List<String> auths = authorizationSet.findAuthorizationsFor(userName);
		final String[] result = new String[auths.size()];
		auths.toArray(result);
		return result;
	}

}
