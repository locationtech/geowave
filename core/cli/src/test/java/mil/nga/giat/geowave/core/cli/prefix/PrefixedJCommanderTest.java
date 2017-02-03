package mil.nga.giat.geowave.core.cli.prefix;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.cli.annotations.PrefixParameter;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslatorTest.ArgumentChildren;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslatorTest.ArgumentChildrenOther;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslatorTest.Arguments;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslatorTest.ArgumentsCollection;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslatorTest.NullDelegate;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslatorTest.PrefixedArguments;
import mil.nga.giat.geowave.core.cli.prefix.PrefixedJCommander.PrefixedJCommanderInitializer;

public class PrefixedJCommanderTest
{

	@Test
	public void testAddCommand() {
		PrefixedJCommander prefixedJCommander = new PrefixedJCommander();

		prefixedJCommander.addCommand(
				"abc",
				(Object) "hello, world",
				"a");
		prefixedJCommander.addCommand(
				"def",
				(Object) "goodbye, world",
				"b");
		prefixedJCommander.parse("abc");
		Assert.assertEquals(
				prefixedJCommander.getParsedCommand(),
				"abc");
	}

	@Test
	public void testNullDelegate() {
		PrefixedJCommander commander = new PrefixedJCommander();
		NullDelegate nullDelegate = new NullDelegate();
		commander.addPrefixedObject(nullDelegate);
		commander.parse();
	}

	@Test
	public void testMapDelegatesPrefix() {
		Arguments args = new Arguments();
		args.argChildren.put(
				"abc",
				new ArgumentChildren());
		args.argChildren.put(
				"def",
				new ArgumentChildren());

		PrefixedJCommander commander = new PrefixedJCommander();
		commander.addPrefixedObject(args);
		commander.parse(
				"--abc.arg",
				"5",
				"--def.arg",
				"blah");

		Assert.assertEquals(
				"5",
				args.argChildren.get("abc").arg);
		Assert.assertEquals(
				"blah",
				args.argChildren.get("def").arg);
	}

	@Test
	public void testCollectionDelegatesPrefix() {
		ArgumentsCollection args = new ArgumentsCollection();
		args.argChildren.add(new ArgumentChildren());
		args.argChildren.add(new ArgumentChildrenOther());

		PrefixedJCommander commander = new PrefixedJCommander();
		commander.addPrefixedObject(args);

		commander.parse(
				"--arg",
				"5",
				"--arg2",
				"blah");

		Assert.assertEquals(
				"5",
				((ArgumentChildren) args.argChildren.get(0)).arg);
		Assert.assertEquals(
				"blah",
				((ArgumentChildrenOther) args.argChildren.get(1)).arg2);
	}

	@Test
	public void testPrefixParameter() {
		PrefixedArguments args = new PrefixedArguments();
		PrefixedJCommander commander = new PrefixedJCommander();
		commander.addPrefixedObject(args);

		commander.parse(
				"--abc.arg",
				"5",
				"--arg",
				"blah");

		Assert.assertEquals(
				"5",
				args.child.arg);
		Assert.assertEquals(
				"blah",
				args.blah);
	}

	@Test
	public void testAddGetPrefixedObjects() {
		PrefixedArguments args = new PrefixedArguments();
		PrefixedJCommander commander = new PrefixedJCommander();
		commander.addPrefixedObject(args);
		Assert.assertTrue(commander.getPrefixedObjects().contains(
				args) && commander.getPrefixedObjects().size() == 1);
	}

	private static class PrefixedArguments
	{
		@ParametersDelegate
		@PrefixParameter(prefix = "abc")
		private ArgumentChildren child = new ArgumentChildren();

		@Parameter(names = "--arg")
		private String blah;
	}

	private static class NullDelegate
	{
		@ParametersDelegate
		private ArgumentChildren value = null;
	}

	private static class ArgumentsCollection
	{
		@ParametersDelegate
		private List<Object> argChildren = new ArrayList<Object>();
	}

	private static class Arguments
	{
		@ParametersDelegate
		private Map<String, ArgumentChildren> argChildren = new HashMap<String, ArgumentChildren>();
	}

	private static class ArgumentChildren
	{
		@Parameter(names = "--arg")
		private String arg;
	}

	private static class ArgumentChildrenOther
	{
		@Parameter(names = "--arg2")
		private String arg2;
	}

}
