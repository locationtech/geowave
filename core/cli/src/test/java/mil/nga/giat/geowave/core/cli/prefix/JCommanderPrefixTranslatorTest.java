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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import org.junit.Assert;
import mil.nga.giat.geowave.core.cli.annotations.PrefixParameter;

public class JCommanderPrefixTranslatorTest
{
	private JCommander prepareCommander(
			JCommanderTranslationMap map ) {
		JCommander commander = new JCommander();
		map.createFacadeObjects();
		for (Object obj : map.getObjects()) {
			commander.addObject(obj);
		}
		return commander;
	}

	@Test
	public void testNullDelegate() {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(new NullDelegate());
		JCommander commander = prepareCommander(translator.translate());
		commander.parse();
	}

	@Test
	public void testMapDelegatesPrefix() {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		Arguments args = new Arguments();
		args.argChildren.put(
				"abc",
				new ArgumentChildren());
		args.argChildren.put(
				"def",
				new ArgumentChildren());
		translator.addObject(args);
		JCommanderTranslationMap map = translator.translate();
		JCommander commander = prepareCommander(map);
		commander.parse(
				"--abc.arg",
				"5",
				"--def.arg",
				"blah");
		map.transformToOriginal();
		Assert.assertEquals(
				"5",
				args.argChildren.get("abc").arg);
		Assert.assertEquals(
				"blah",
				args.argChildren.get("def").arg);
	}

	@Test
	public void testCollectionDelegatesPrefix() {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		ArgumentsCollection args = new ArgumentsCollection();
		args.argChildren.add(new ArgumentChildren());
		args.argChildren.add(new ArgumentChildrenOther());
		translator.addObject(args);
		JCommanderTranslationMap map = translator.translate();
		JCommander commander = prepareCommander(map);
		commander.parse(
				"--arg",
				"5",
				"--arg2",
				"blah");
		map.transformToOriginal();
		Assert.assertEquals(
				"5",
				((ArgumentChildren) args.argChildren.get(0)).arg);
		Assert.assertEquals(
				"blah",
				((ArgumentChildrenOther) args.argChildren.get(1)).arg2);
	}

	@Test
	public void testPrefixParameter() {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		PrefixedArguments args = new PrefixedArguments();
		translator.addObject(args);
		JCommanderTranslationMap map = translator.translate();
		JCommander commander = prepareCommander(map);
		commander.parse(
				"--abc.arg",
				"5",
				"--arg",
				"blah");
		map.transformToOriginal();
		Assert.assertEquals(
				"5",
				args.child.arg);
		Assert.assertEquals(
				"blah",
				args.blah);
	}

	public static class PrefixedArguments
	{
		@ParametersDelegate
		@PrefixParameter(prefix = "abc")
		private ArgumentChildren child = new ArgumentChildren();

		@Parameter(names = "--arg")
		private String blah;
	}

	public static class NullDelegate
	{
		@ParametersDelegate
		private ArgumentChildren value = null;
	}

	public static class ArgumentsCollection
	{
		@ParametersDelegate
		private List<Object> argChildren = new ArrayList<Object>();
	}

	public static class Arguments
	{
		@ParametersDelegate
		private Map<String, ArgumentChildren> argChildren = new HashMap<String, ArgumentChildren>();
	}

	public static class ArgumentChildren
	{
		@Parameter(names = "--arg")
		private String arg;
	}

	public static class ArgumentChildrenOther
	{
		@Parameter(names = "--arg2")
		private String arg2;
	}
}
