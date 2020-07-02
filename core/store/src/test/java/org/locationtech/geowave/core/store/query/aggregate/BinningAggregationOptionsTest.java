package org.locationtech.geowave.core.store.query.aggregate;

import org.junit.Test;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class BinningAggregationOptionsTest {

  @Test
  public void testSerialization() {
    BinningAggregationOptions<?, ?> opts =
        new BinningAggregationOptions<>(new byte[0], null, null, 1234);
    assertThat(opts.baseBytes, is(new byte[0]));
    assertThat(opts.baseParams, is(nullValue()));
    assertThat(opts.binningStrategy, is(nullValue()));
    assertThat(opts.maxBins, is(1234));

    byte[] serialized = PersistenceUtils.toBinary(opts);
    BinningAggregationOptions<?, ?> roundtripped =
        (BinningAggregationOptions<?, ?>) PersistenceUtils.fromBinary(serialized);

    assertThat(opts.baseBytes, is(roundtripped.baseBytes));
    assertThat(opts.baseParams, is(roundtripped.baseParams));
    assertThat(opts.binningStrategy, is(roundtripped.binningStrategy));
    assertThat(opts.maxBins, is(roundtripped.maxBins));

    // build some blank objects just for serialization purposes,
    // to ensure its not just making everything null.
    Persistable blankPersistable = new Persistable() {
      @Override
      public byte[] toBinary() {
        return new byte[0];
      }

      @Override
      public void fromBinary(byte[] bytes) {

      }
    };

    AggregationBinningStrategy<Object> blankStrategy = new AggregationBinningStrategy<Object>() {
      @Override
      public String[] binEntry(Object entry) {
        return new String[0];
      }

      @Override
      public byte[] toBinary() {
        return new byte[0];
      }

      @Override
      public void fromBinary(byte[] bytes) {

      }
    };

    opts =
        new BinningAggregationOptions<>(
            new byte[] {0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE},
            blankPersistable,
            blankStrategy,
            Integer.MAX_VALUE);
    serialized = PersistenceUtils.toBinary(opts);
    roundtripped = (BinningAggregationOptions<?, ?>) PersistenceUtils.fromBinary(serialized);

    assertThat(opts.baseBytes, is(roundtripped.baseBytes));
    assertThat(opts.baseParams, is(notNullValue()));
    assertThat(opts.binningStrategy, is(notNullValue()));
    assertThat(opts.maxBins, is(roundtripped.maxBins));
  }
}
