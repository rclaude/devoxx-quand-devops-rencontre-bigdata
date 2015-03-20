package fr.devoxx.devops.logs.spark;

import fr.devoxx.devops.logs.SparkTest;
import org.junit.Test;
import scala.Tuple4;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

public class Spark5Test extends SparkTest {
    @Test
    public void test_spark() {
        Spark5 test = new Spark5();
        Tuple4<Long, Double, Double, Double> result = test.process(rdd());
        assertThat(result._1(), is(6102L));
        assertThat(result._2(), is(40.0));
        assertThat(result._3(), closeTo(90.044, 0.001));
        assertThat(result._4(), is(139.0));
    }

}
