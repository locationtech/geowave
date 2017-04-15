package mil.nga.giat.geowave.core.cli.spi;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.ExplainCommand;

public class OperationRegistryTest
{

	@Test
	public void testGetOperation() {
		OperationEntry optentry = new OperationEntry(
				ExplainCommand.class);
		List<OperationEntry> entries = new ArrayList<OperationEntry>();
		entries.add(optentry);
		OperationRegistry optreg = new OperationRegistry(
				entries);

		assertEquals(
				"explain",
				optreg.getOperation(
						ExplainCommand.class).getOperationName());
		assertEquals(
				true,
				optreg.getAllOperations().contains(
						optentry));
	}

}
