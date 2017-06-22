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
package mil.nga.giat.geowave.core.cli.parser;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import org.junit.Assert;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.spi.OperationEntry;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class OperationParserTest
{
	@Test
	public void testParseTopLevel() {

		OperationEntry op1Entry = new OperationEntry(
				Op1.class);
		OperationEntry op2Entry = new OperationEntry(
				Op2.class);
		op1Entry.addChild(op2Entry);

		List<OperationEntry> entries = new ArrayList<OperationEntry>();
		entries.add(op1Entry);
		entries.add(op2Entry);

		OperationParser parser = new OperationParser(
				new OperationRegistry(
						entries));

		CommandLineOperationParams params = parser.parse(
				Op1.class,
				new String[] {
					"op",
					"--username",
					"user",
					"--password",
					"blah"
				});

		Op2 op2 = (Op2) params.getOperationMap().get(
				"op");

		Assert.assertEquals(
				"blah",
				op2.args.passWord);
		Assert.assertEquals(
				"user",
				op2.args.userName);

	}

	@Test
	public void testParseArgs() {
		OperationParser parser = new OperationParser();
		Args args = new Args();
		parser.addAdditionalObject(args);
		parser.parse(new String[] {
			"--username",
			"user",
			"--password",
			"blah"
		});
		Assert.assertEquals(
				"blah",
				args.passWord);
		Assert.assertEquals(
				"user",
				args.userName);
	}

	@Test
	public void testParseOperation() {

		OperationEntry op1Entry = new OperationEntry(
				Op1.class);
		OperationEntry op2Entry = new OperationEntry(
				Op2.class);
		op1Entry.addChild(op2Entry);

		List<OperationEntry> entries = new ArrayList<OperationEntry>();
		entries.add(op1Entry);
		entries.add(op2Entry);

		OperationParser parser = new OperationParser(
				new OperationRegistry(
						entries));

		Op2 op2 = new Op2();

		parser.parse(
				op2,
				new String[] {
					"--username",
					"user",
					"--password",
					"blah"
				});

		Assert.assertEquals(
				"blah",
				op2.args.passWord);
		Assert.assertEquals(
				"user",
				op2.args.userName);
	}

	public static class Args
	{
		@Parameter(names = "--username")
		private String userName;

		@Parameter(names = "--password")
		private String passWord;
	}

	@GeowaveOperation(name = "toplevel")
	public static class Op1 extends
			DefaultOperation
	{
	}

	@GeowaveOperation(name = "op", parentOperation = Op1.class)
	public static class Op2 extends
			DefaultOperation implements
			Command
	{

		@ParametersDelegate
		private Args args = new Args();

		@Override
		public void execute(
				OperationParams params )
				throws Exception {

		}
	}

}
