package mil.nga.giat.geowave.core.cli.prefix;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.cli.annotations.PrefixParameter;

public class JCommanderPropertiesTransformerTest
{

	@Test
	public void testWithoutDelegate() {
		Args args = new Args();
		args.passWord = "blah";
		args.userName = "user";
		JCommanderPropertiesTransformer transformer = new JCommanderPropertiesTransformer();
		transformer.addObject(args);
		Map<String, String> props = new HashMap<String, String>();
		transformer.transformToMap(props);
		Assert.assertEquals(
				2,
				props.size());
		Assert.assertEquals(
				"blah",
				props.get("password"));
		Assert.assertEquals(
				"user",
				props.get("username"));
	}

	@Test
	public void testWithDelegate() {
		DelegateArgs args = new DelegateArgs();
		args.args.passWord = "blah";
		args.args.userName = "user";
		args.additional = "add";
		JCommanderPropertiesTransformer transformer = new JCommanderPropertiesTransformer();
		transformer.addObject(args);
		Map<String, String> props = new HashMap<String, String>();
		transformer.transformToMap(props);
		Assert.assertEquals(
				3,
				props.size());
		Assert.assertEquals(
				"blah",
				props.get("password"));
		Assert.assertEquals(
				"user",
				props.get("username"));
		Assert.assertEquals(
				"add",
				props.get("additional"));
	}

	@Test
	public void testWithPrefix() {
		DelegatePrefixArgs args = new DelegatePrefixArgs();
		args.args.passWord = "blah";
		args.args.userName = "user";
		args.additional = "add";
		JCommanderPropertiesTransformer transformer = new JCommanderPropertiesTransformer();
		transformer.addObject(args);
		Map<String, String> props = new HashMap<String, String>();
		transformer.transformToMap(props);
		Assert.assertEquals(
				3,
				props.size());
		Assert.assertEquals(
				"blah",
				props.get("abc.password"));
		Assert.assertEquals(
				"user",
				props.get("abc.username"));
		Assert.assertEquals(
				"add",
				props.get("additional"));
	}

	public class Args
	{
		@Parameter(names = "--username")
		private String userName;

		@Parameter(names = "--password")
		private String passWord;
	}

	public class DelegateArgs
	{
		@ParametersDelegate
		private Args args = new Args();

		@Parameter(names = "--additional")
		private String additional;
	}

	public class DelegatePrefixArgs
	{
		@ParametersDelegate
		@PrefixParameter(prefix = "abc")
		private Args args = new Args();

		@Parameter(names = "--additional")
		private String additional;
	}

}
