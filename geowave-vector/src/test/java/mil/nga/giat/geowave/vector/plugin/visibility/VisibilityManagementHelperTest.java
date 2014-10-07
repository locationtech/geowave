package mil.nga.giat.geowave.vector.plugin.visibility;

import static org.junit.Assert.*;
import mil.nga.giat.geowave.vector.plugin.visibility.VisibilityManagementHelper;

import org.junit.Test;

public class VisibilityManagementHelperTest
{

	@Test
	public void test() {
		assertNotNull(VisibilityManagementHelper.loadVisibilityManagement());
	}

}
