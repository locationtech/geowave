/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import org.junit.Assert;
import org.junit.Test;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class JCommanderTranslationMapTest {
  @Test
  public void testCreateFacadesWithoutDelegate() {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    translator.addObject(new ArgumentChildren());
    final JCommanderTranslationMap map = translator.translate();
    map.createFacadeObjects();
    Assert.assertEquals(1, map.getObjects().size());
  }

  @Test
  public void testCreateFacadesWithDelegate() {
    final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
    translator.addObject(new Arguments());
    final JCommanderTranslationMap map = translator.translate();
    map.createFacadeObjects();
    Assert.assertEquals(2, map.getObjects().size());
  }

  public static class Arguments {
    @ParametersDelegate
    private final ArgumentChildren children = new ArgumentChildren();

    @Parameter(names = "--arg2")
    private String arg2;
  }

  public static class ArgumentChildren {
    @Parameter(names = "--arg")
    private String arg;
  }
}
