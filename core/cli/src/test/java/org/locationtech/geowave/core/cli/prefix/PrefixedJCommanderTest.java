/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.cli.annotations.PrefixParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class PrefixedJCommanderTest {

  @Test
  public void testAddCommand() {
    final PrefixedJCommander prefixedJCommander = new PrefixedJCommander();

    prefixedJCommander.addCommand("abc", (Object) "hello, world", "a");
    prefixedJCommander.addCommand("def", (Object) "goodbye, world", "b");
    prefixedJCommander.parse("abc");
    Assert.assertEquals(prefixedJCommander.getParsedCommand(), "abc");
  }

  @Test
  public void testNullDelegate() {
    final PrefixedJCommander commander = new PrefixedJCommander();
    final NullDelegate nullDelegate = new NullDelegate();
    commander.addPrefixedObject(nullDelegate);
    commander.parse();
  }

  @Test
  public void testMapDelegatesPrefix() {
    final Arguments args = new Arguments();
    args.argChildren.put("abc", new ArgumentChildren());
    args.argChildren.put("def", new ArgumentChildren());

    final PrefixedJCommander commander = new PrefixedJCommander();
    commander.addPrefixedObject(args);
    commander.parse("--abc.arg", "5", "--def.arg", "blah");

    Assert.assertEquals("5", args.argChildren.get("abc").arg);
    Assert.assertEquals("blah", args.argChildren.get("def").arg);
  }

  @Test
  public void testCollectionDelegatesPrefix() {
    final ArgumentsCollection args = new ArgumentsCollection();
    args.argChildren.add(new ArgumentChildren());
    args.argChildren.add(new ArgumentChildrenOther());

    final PrefixedJCommander commander = new PrefixedJCommander();
    commander.addPrefixedObject(args);

    commander.parse("--arg", "5", "--arg2", "blah");

    Assert.assertEquals("5", ((ArgumentChildren) args.argChildren.get(0)).arg);
    Assert.assertEquals("blah", ((ArgumentChildrenOther) args.argChildren.get(1)).arg2);
  }

  @Test
  public void testPrefixParameter() {
    final PrefixedArguments args = new PrefixedArguments();
    final PrefixedJCommander commander = new PrefixedJCommander();
    commander.addPrefixedObject(args);

    commander.parse("--abc.arg", "5", "--arg", "blah");

    Assert.assertEquals("5", args.child.arg);
    Assert.assertEquals("blah", args.blah);
  }

  @Test
  public void testAddGetPrefixedObjects() {
    final PrefixedArguments args = new PrefixedArguments();
    final PrefixedJCommander commander = new PrefixedJCommander();
    commander.addPrefixedObject(args);
    Assert.assertTrue(
        commander.getPrefixedObjects().contains(args)
            && (commander.getPrefixedObjects().size() == 1));
  }

  private static class PrefixedArguments {
    @ParametersDelegate
    @PrefixParameter(prefix = "abc")
    private final ArgumentChildren child = new ArgumentChildren();

    @Parameter(names = "--arg")
    private String blah;
  }

  private static class NullDelegate {
    @ParametersDelegate
    private final ArgumentChildren value = null;
  }

  private static class ArgumentsCollection {
    @ParametersDelegate
    private final List<Object> argChildren = new ArrayList<>();
  }

  private static class Arguments {
    @ParametersDelegate
    private final Map<String, ArgumentChildren> argChildren = new HashMap<>();
  }

  private static class ArgumentChildren {
    @Parameter(names = "--arg")
    private String arg;
  }

  private static class ArgumentChildrenOther {
    @Parameter(names = "--arg2")
    private String arg2;
  }
}
