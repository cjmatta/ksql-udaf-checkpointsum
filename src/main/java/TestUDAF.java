import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "testudaf", description = "Test udaf.")
public class TestUDAF  {


  @UdafFactory(description = "test UDAF", paramSchema = "STRUCT<TYPE VARCHAR, AMOUNT DOUBLE>")
  public static Udaf<Struct, Double, Double> getUDAF() {
    return new Udaf<Struct, Double, Double>() {
      public Double initialize() {
        return 0.0;
      }

      public Double aggregate(final Struct struct, final Double aDouble) {
        if (struct.get("TYPE").equals("delta")) {
          return aDouble + (Double) struct.get("AMOUNT");
        } else if (struct.get("TYPE").equals("state")) {
          return (Double) struct.get("AMOUNT");
        }
        System.err.println("Invalid type: " + struct.get("TYPE"));
        return null;
      }

      public Double merge(final Double aDouble, final Double a1) {
        return null;
      }

      public Double map(final Double aDouble) {
        return aDouble;
      }
    };
  }

}
