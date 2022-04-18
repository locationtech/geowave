/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.prefix;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.cli.annotations.PrefixParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class JCommanderPropertiesTransformerTest {

  @Test
  public void testWithoutDelegate() {
    final Args args = new Args();
    args.passWord = "blah";
    args.userName = "user";
    final JCommanderPropertiesTransformer transformer = new JCommanderPropertiesTransformer();
    transformer.addObject(args);
    final Map<String, String> props = new HashMap<>();
    transformer.transformToMap(props);
    Assert.assertEquals(2, props.size());
    Assert.assertEquals("blah", props.get("password"));
    Assert.assertEquals("user", props.get("username"));
  }

  @Test
  public void testWithDelegate() {
    final DelegateArgs args = new DelegateArgs();
    args.args.passWord = "blah";
    args.args.userName = "user";
    args.additional = "add";
    final JCommanderPropertiesTransformer transformer = new JCommanderPropertiesTransformer();
    transformer.addObject(args);
    final Map<String, String> props = new HashMap<>();
    transformer.transformToMap(props);
    Assert.assertEquals(3, props.size());
    Assert.assertEquals("blah", props.get("password"));
    Assert.assertEquals("user", props.get("username"));
    Assert.assertEquals("add", props.get("additional"));
  }

  @Test
  public void testWithPrefix() {
    final DelegatePrefixArgs args = new DelegatePrefixArgs();
    args.args.passWord = "blah";
    args.args.userName = "user";
    args.additional = "add";
    final JCommanderPropertiesTransformer transformer = new JCommanderPropertiesTransformer();
    transformer.addObject(args);
    final Map<String, String> props = new HashMap<>();
    transformer.transformToMap(props);
    Assert.assertEquals(3, props.size());
    Assert.assertEquals("blah", props.get("abc.password"));
    Assert.assertEquals("user", props.get("abc.username"));
    Assert.assertEquals("add", props.get("additional"));
  }

  public class Args {
    @Parameter(names = "--username")
    private String userName;

    @Parameter(names = "--password")
    private String passWord;
  }

  public class DelegateArgs {
    @ParametersDelegate
    private final Args args = new Args();

    @Parameter(names = "--additional")
    private String additional;
  }

  public class DelegatePrefixArgs {
    @ParametersDelegate
    @PrefixParameter(prefix = "abc")
    private final Args args = new Args();

    @Parameter(names = "--additional")
    private String additional;
  }
}
