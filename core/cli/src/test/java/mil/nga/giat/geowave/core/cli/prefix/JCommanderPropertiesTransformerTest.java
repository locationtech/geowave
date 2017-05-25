/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.cli.prefix;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import org.junit.Assert;
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
