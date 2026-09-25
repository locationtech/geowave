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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
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

  @Test
  public void testTransformFromMapConcurrently() throws Exception {
    final Map<String, String> props = new HashMap<>();
    for (int i = 0; i < 8; i++) {
      props.put("field" + i, "value" + i);
    }
    final int threads = 16;
    final AtomicInteger incomplete = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(pool.submit(() -> {
          start.await();
          for (int i = 0; i < 2000; i++) {
            final ManyArgs args = new ManyArgs();
            final JCommanderPropertiesTransformer transformer =
                new JCommanderPropertiesTransformer();
            transformer.addObject(args);
            transformer.transformFromMap(props);
            if (!args.isComplete()) {
              incomplete.incrementAndGet();
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> f : futures) {
        f.get();
      }
    } finally {
      pool.shutdown();
    }
    Assert.assertEquals(0, incomplete.get());
  }

  public static class ManyArgs {
    @Parameter(names = "--field0")
    private String field0;
    @Parameter(names = "--field1")
    private String field1;
    @Parameter(names = "--field2")
    private String field2;
    @Parameter(names = "--field3")
    private String field3;
    @Parameter(names = "--field4")
    private String field4;
    @Parameter(names = "--field5")
    private String field5;
    @Parameter(names = "--field6")
    private String field6;
    @Parameter(names = "--field7")
    private String field7;

    private boolean isComplete() {
      return "value0".equals(field0)
          && "value1".equals(field1)
          && "value2".equals(field2)
          && "value3".equals(field3)
          && "value4".equals(field4)
          && "value5".equals(field5)
          && "value6".equals(field6)
          && "value7".equals(field7);
    }
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
