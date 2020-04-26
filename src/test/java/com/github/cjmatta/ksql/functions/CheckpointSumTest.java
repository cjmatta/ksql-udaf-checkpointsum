package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CheckpointSumTest {
  private final Schema INPUT_STRUCT = SchemaBuilder.struct().optional()
    .field(CheckpointSum.TYPE, Schema.STRING_SCHEMA)
    .field(CheckpointSum.VALUE, Schema.FLOAT64_SCHEMA)
    .build();


  @Test
  public void shouldAddDeltas() {

    Udaf<Struct, Struct, Double> udaf = CheckpointSum.checkpointSum();
    Struct aggregate = udaf.initialize();

    Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d)
    };

    for (Struct thisValue: values) {
      aggregate = udaf.aggregate(thisValue, aggregate);
    }

    assertEquals(3.0d, aggregate.getFloat64(CheckpointSum.VALUE), 0);

  }

  @Test
  public void shouldAddAbsolutes() {

    Udaf<Struct, Struct, Double> udaf = CheckpointSum.checkpointSum();
    Struct aggregate = udaf.initialize();

    Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 10.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d)
    };

    for (Struct thisValue: values) {
      aggregate = udaf.aggregate(thisValue, aggregate);
    }

    assertEquals(12.0d, aggregate.getFloat64(CheckpointSum.VALUE), 0);
  }

  @Test
  public void shouldTakeLatestAbsolute() {

    Udaf<Struct, Struct, Double> udaf = CheckpointSum.checkpointSum();
    Struct aggregate = udaf.initialize();

    Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 10.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 20.0d)
    };

    for (Struct thisValue: values) {
      aggregate = udaf.aggregate(thisValue, aggregate);
    }

    assertEquals(20.0d, aggregate.getFloat64(CheckpointSum.VALUE), 0);
  }

}

