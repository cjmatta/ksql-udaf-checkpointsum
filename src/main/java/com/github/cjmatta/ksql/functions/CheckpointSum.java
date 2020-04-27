package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "checkpoint_sum", description = "Computes SUM of a stream of records where some are the absolute value, and some are the delta")
public class CheckpointSum {
  public static final String TYPE = "TYPE";
  public static final String VALUE = "VALUE";
  public static final String TYPE_ABSOLUTE = "absolute";
  public static final String TYPE_DELTA = "delta";

  private static final Schema AGGREGATE_STRUCT = SchemaBuilder.struct().optional()
      .field(TYPE, Schema.STRING_SCHEMA)
      .field(VALUE, Schema.FLOAT64_SCHEMA)
      .build();

  @UdafFactory(description = "Compute the sum or a series of records representing either the absolute value or delta",
      paramSchema = "STRUCT<TYPE STRING, VALUE DOUBLE>", aggregateSchema = "STRUCT<TYPE STRING, VALUE DOUBLE>")
  public static Udaf<Struct, Struct, Double> checkpointSum() {

    return new Udaf<Struct, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(AGGREGATE_STRUCT).put(TYPE, TYPE_ABSOLUTE).put(VALUE, 0.0d);
      }

      @Override
      public Struct aggregate(final Struct input, final Struct aggregate) {
        Object obj = input.get(TYPE);
//        if (obj == null) {
//          return null;
//        }

        String inputType = obj.toString();


//        Return null if the value of TYPE isn't either TYPE_ABSOLUTE or TYPE_DELTA
        if (!(inputType.equals(TYPE_ABSOLUTE) || inputType.equals(TYPE_DELTA))) {
          return null;
        }

        Double value = input.getFloat64(VALUE);

        if (inputType.equals(TYPE_ABSOLUTE)) {
          return aggregate.put(TYPE, TYPE_ABSOLUTE).put(VALUE, value);
        } else {
          return aggregate
              .put(TYPE, TYPE_DELTA)
              .put(VALUE, aggregate.getFloat64(VALUE) + value);
        }
      }

      @Override
      public Struct merge(Struct agg1, Struct agg2) {
        String agg2Type = agg2.get(TYPE).toString();

        if (agg2Type.equals(TYPE_ABSOLUTE)) {
          return agg2;
        } else {
          return agg2.put(TYPE, TYPE_DELTA)
              .put(VALUE, agg1.getFloat64(VALUE) + agg2.getFloat64(VALUE));
        }
      }

      @Override
      public Double map(Struct agg) {
        return agg.getFloat64(VALUE);
      }
    };
  }
}
