import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.expressions.javalang.typed.avg;

/**
 * This class will be use for A poc of spark
 * Created by fabrice on 4/29/19.
 */
public class SparkClients {
    private static final SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();
    public static void main(String[] args) {
        Dataset<Row> clients = sumAmount(spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(args[0]));
        clients.show();

    }

    /**
     * Calculate the sum of amount of client
     * @param dsClients
     * @return
     */
    public static Dataset<Row> sumAmount(Dataset<Row> dsClients) {
        return dsClients
                .groupBy("customerId")
                .sum("amount")
                .orderBy("customerId");
    }


}
