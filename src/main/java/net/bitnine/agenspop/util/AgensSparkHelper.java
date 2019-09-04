package net.bitnine.agenspop.util;

import net.bitnine.agenspop.dto.DataSetResult;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class AgensSparkHelper {

    // **NOTE
    // https://github.com/uber/uberscriptquery/blob/master/src/main/java/com/uber/uberscriptquery/util/SparkUtils.java
/*
    public static DataSetResult getDataSetResult(Dataset<Row> df) {
        DataSetResult result = new DataSetResult();

        String[] fieldNames = df.schema().fieldNames();

        result.getColumnNames().addAll(Arrays.asList(fieldNames));

        Row[] rows = (Row[]) df.collect();
        for (Row row : rows) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < fieldNames.length; i++) {
                Object obj = row.get(i);
                values.add(obj);
            }
            result.getRows().add(values);
        }

        return result;
    }
*/

}
