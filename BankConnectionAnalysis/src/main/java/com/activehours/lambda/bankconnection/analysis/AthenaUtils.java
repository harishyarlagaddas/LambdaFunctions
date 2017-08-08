package com.activehours.lambda.bankconnection.analysis;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Map;

public class AthenaUtils {
    static String ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING =
            "ALTER TABLE %TABLE_NAME ADD IF NOT EXISTS PARTITION "
                    + "(year=%YY, month=%MM, day=%DD, hour=%HH) "
                    + "LOCATION \"s3://%S3_BUCKET_NAME/%YY/%MM/%DD/%HH/\"";

    public static void addPartitionToAthenaTables(
            Connection connection, String tableName, String bucketName,
            Calendar calendar, LambdaLogger logger)
            throws SQLException {

        DecimalFormat numberFormat= new DecimalFormat("00");
        numberFormat.setRoundingMode(RoundingMode.DOWN);

        String estYear = String.valueOf(calendar.get(Calendar.YEAR));
        String estMonth = numberFormat.format(calendar.get(Calendar.MONTH)+1);
        String estDay = numberFormat.format(calendar.get(Calendar.DAY_OF_MONTH));
        String estHour = numberFormat.format(calendar.get(Calendar.HOUR_OF_DAY));

        // Add hourly partition to Athena Analysis table.
        String hourlyPartitionSqlStr = ATHENA_TABLE_HOURLY_PARTITION_SQL_STRING
                .replaceAll("%TABLE_NAME", tableName)
                .replaceAll("%S3_BUCKET_NAME", bucketName)
                .replaceAll("%YY", estYear)
                .replaceAll("%MM", estMonth)
                .replaceAll("%DD", estDay)
                .replaceAll("%HH", estHour);

        logger.log("Creating the hourly partition for athena analysis table. hourlyPartitionSqlStr: " + hourlyPartitionSqlStr);
        executeAthenaStatement(connection, hourlyPartitionSqlStr, logger);
        logger.log("Creation of hourly partition for athena analysis table succeeded.");
    }

    public static ResultSet executeAthenaStatement(Connection connection, String sqlCommand, LambdaLogger logger)
            throws SQLException {
        logger.log("Executing SQL Statement: "+sqlCommand);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sqlCommand);
        logger.log("Completed the execution of sql statement: "+sqlCommand+" Result: "+result);

        if (statement != null) {
            statement.close();
        }
        return result;
    }
}
