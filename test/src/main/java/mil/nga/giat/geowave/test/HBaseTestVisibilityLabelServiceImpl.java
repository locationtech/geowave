package mil.nga.giat.geowave.test;

import java.io.IOException;

import org.apache.hadoop.hbase.security.visibility.DefaultVisibilityLabelServiceImpl;

/**
 * This class exists to circumvent the issue with the Visibility IT failing when
 * the user running the test is a superuser.
 * 
 * @author kent
 *
 */
public class HBaseTestVisibilityLabelServiceImpl extends
		DefaultVisibilityLabelServiceImpl
{
	@Override
	protected boolean isReadFromSystemAuthUser()
			throws IOException {
		return false;
	}
}
