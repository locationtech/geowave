package mil.nga.giat.geowave.adapter.vector.auth;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

import mil.nga.giat.geowave.adapter.vector.auth.AuthorizationSPI;
import mil.nga.giat.geowave.adapter.vector.auth.JsonFileAuthorizationFactory;

import org.geoserver.security.impl.GeoServerUser;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

public class JsonFileAuthorizationAdapterTest
{

	@Test
	public void testBasic()
			throws MalformedURLException {
		SecurityContext context = new SecurityContext() {

			@Override
			public Authentication getAuthentication() {
				Authentication auth = new UsernamePasswordAuthenticationToken(
						"fred",
						"barney");
				return auth;
			}

			@Override
			public void setAuthentication(
					Authentication arg0 ) {}

		};
		SecurityContextHolder.setContext(context);
		File cwd = new File(
				".");
		AuthorizationSPI authProvider = new JsonFileAuthorizationFactory().create(new URL(
				"file://" + cwd.getAbsolutePath() + "/src/test/resources/jsonAuthfile.json"));
		assertTrue(Arrays.equals(
				new String[] {
					"1",
					"2",
					"3"
				},
				authProvider.getAuthorizations()));

	}

	@Test
	public void testUserDetails()
			throws MalformedURLException {
		final UserDetails ud = new GeoServerUser(
				"fred");
		SecurityContext context = new SecurityContext() {

			@Override
			public Authentication getAuthentication() {
				Authentication auth = new UsernamePasswordAuthenticationToken(
						ud,
						"barney");
				return auth;
			}

			@Override
			public void setAuthentication(
					Authentication arg0 ) {}

		};
		SecurityContextHolder.setContext(context);
		File cwd = new File(
				".");
		AuthorizationSPI authProvider = new JsonFileAuthorizationFactory().create(new URL(
				"file://" + cwd.getAbsolutePath() + "/src/test/resources/jsonAuthfile.json"));
		assertTrue(Arrays.equals(
				new String[] {
					"1",
					"2",
					"3"
				},
				authProvider.getAuthorizations()));

	}

}
