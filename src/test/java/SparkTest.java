import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.Metadata.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Created by fabrice on 4/29/19.
 */
public class SparkTest {

    @Rule
    public SparkTestSession spark = new SparkTestSession();
    private Dataset<Row> clients;

    @Before
    public void before(){
        List<Row> rows = Arrays.asList(
                RowFactory.create("ABAA", 100L),
                RowFactory.create("ABAA", 150L),
                RowFactory.create("CCC", 70L));

        StructField customerId = new StructField("customerId", StringType, false, empty());
        StructField amount = new StructField("amount", LongType, false, empty());
        StructType structType = new StructType(new StructField[]{customerId, amount});

        clients = spark.getSpark().createDataFrame(rows, structType);
    }


    @Test
    public void shouldReturnTheCorrectSumForClient(){

        Dataset<Row> sumClients = SparkClients.sumAmount(clients);

        assertEquals(2, sumClients.count());
       assertEquals("ABAA", sumClients.first().getAs("customerId"));
       sumClients.printSchema();
       assertEquals(250, (long) sumClients.first().<Long>getAs("sum(amount)"));
       sumClients.foreach((ForeachFunction<Row>) row -> assertNotNull(row.getAs("customerId")));
    }
}
