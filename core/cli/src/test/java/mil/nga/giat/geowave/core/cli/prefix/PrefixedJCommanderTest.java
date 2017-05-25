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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import org.junit.Assert;
import mil.nga.giat.geowave.core.cli.annotations.PrefixParameter;

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
