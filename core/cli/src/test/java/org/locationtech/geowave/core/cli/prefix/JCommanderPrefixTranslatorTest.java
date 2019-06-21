/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
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
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class JCommanderPrefixTranslatorTest {
  private JCommander prepareCommander(final JCommanderTranslationMap map) {
    final JCommander commander = new JCommander();
    map.createFacadeObjects();
    for (final Object obj : map.getObjects()) {
      commander.addObject(obj);
    }
    return commander;
  }

  @Test
  public void testNullDelegate() {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    translator.addObject(new NullDelegate());
    final JCommander commander = prepareCommander(translator.translate());
    commander.parse();
  }

  @Test
  public void testMapDelegatesPrefix() {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    final Arguments args = new Arguments();
    args.argChildren.put("abc", new ArgumentChildren());
    args.argChildren.put("def", new ArgumentChildren());
    translator.addObject(args);
    final JCommanderTranslationMap map = translator.translate();
    final JCommander commander = prepareCommander(map);
    commander.parse("--abc.arg", "5", "--def.arg", "blah");
    map.transformToOriginal();
    Assert.assertEquals("5", args.argChildren.get("abc").arg);
    Assert.assertEquals("blah", args.argChildren.get("def").arg);
  }

  @Test
  public void testCollectionDelegatesPrefix() {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    final ArgumentsCollection args = new ArgumentsCollection();
    args.argChildren.add(new ArgumentChildren());
    args.argChildren.add(new ArgumentChildrenOther());
    translator.addObject(args);
    final JCommanderTranslationMap map = translator.translate();
    final JCommander commander = prepareCommander(map);
    commander.parse("--arg", "5", "--arg2", "blah");
    map.transformToOriginal();
    Assert.assertEquals("5", ((ArgumentChildren) args.argChildren.get(0)).arg);
    Assert.assertEquals("blah", ((ArgumentChildrenOther) args.argChildren.get(1)).arg2);
  }

  @Test
  public void testPrefixParameter() {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    final PrefixedArguments args = new PrefixedArguments();
    translator.addObject(args);
    final JCommanderTranslationMap map = translator.translate();
    final JCommander commander = prepareCommander(map);
    commander.parse("--abc.arg", "5", "--arg", "blah");
    map.transformToOriginal();
    Assert.assertEquals("5", args.child.arg);
    Assert.assertEquals("blah", args.blah);
  }

  public static class PrefixedArguments {
    @ParametersDelegate
    @PrefixParameter(prefix = "abc")
    private final ArgumentChildren child = new ArgumentChildren();

    @Parameter(names = "--arg")
    private String blah;
  }

  public static class NullDelegate {
    @ParametersDelegate
    private final ArgumentChildren value = null;
  }

  public static class ArgumentsCollection {
    @ParametersDelegate
    private final List<Object> argChildren = new ArrayList<>();
  }

  public static class Arguments {
    @ParametersDelegate
    private final Map<String, ArgumentChildren> argChildren = new HashMap<>();
  }

  public static class ArgumentChildren {
    @Parameter(names = "--arg")
    private String arg;
  }

  public static class ArgumentChildrenOther {
    @Parameter(names = "--arg2")
    private String arg2;
  }
}
