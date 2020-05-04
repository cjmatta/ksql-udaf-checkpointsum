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

    Udaf<Struct, Double, Double> udaf = CheckpointSum.checkpointSum();
    Double aggregate = udaf.initialize();

    Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d)
    };

    for (Struct thisValue: values) {
      aggregate = udaf.aggregate(thisValue, aggregate);
    }

    assertEquals(3.0d, aggregate, 0);

  }

  @Test
  public void shouldAddAbsolutes() {

    Udaf<Struct, Double, Double> udaf = CheckpointSum.checkpointSum();
    Double aggregate = udaf.initialize();

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

    assertEquals(12.0d, aggregate, 0);
  }

  @Test
  public void shouldTakeLatestAbsolute() {

    Udaf<Struct, Double, Double> udaf = CheckpointSum.checkpointSum();
    Double aggregate = udaf.initialize();

    Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 10.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 20.0d)
    };

    for (Struct thisValue: values) {
      aggregate = udaf.aggregate(thisValue, aggregate);
    }

    assertEquals(20.0d, aggregate, 0);
  }

}

