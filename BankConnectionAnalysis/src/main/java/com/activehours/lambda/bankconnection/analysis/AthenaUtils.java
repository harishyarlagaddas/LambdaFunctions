package com.activehours.lambda.bankconnection.analysis;

import com.activehours.lambda.bankconnection.analysis.Model.Incoming.IncomingBankConnectionEvent;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class AthenaUtils {
    public static String SQL_STATEMENT_PLACE_HOLDER_TABLE_NAME = "%TABLE_NAME";
    public static String SQL_STATEMENT_PLACE_HOLDER_YEAR = "%YYYY";
    public static String SQL_STATEMENT_PLACE_HOLDER_MONTH = "%MM";
    public static String SQL_STATEMENT_PLACE_HOLDER_DAY = "%DD";
    public static String SQL_STATEMENT_PLACE_HOLDER_HOUR = "%HH";
    public static String SQL_STATEMENT_PLACE_HOLDER_FROM_TIME = "%FROM_TIME";
    public static String SQL_STATEMENT_PLACE_HOLDER_TO_TIME = "%TO_TIME";
    public static String SQL_STATEMENT_PLACE_HOLDER_FROM_ROW_NUMBER = "%FROM_ROW_NUMBER";
    public static String SQL_STATEMENT_PLACE_HOLDER_TO_ROW_NUMBER = "%TO_ROW_NUMBER";
    public static String SQL_STATEMENT_PLACE_HOLDER_USER_IDS = "%USER_IDS";

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

    public static String FillHourlyPartitionsInSqlStatement(String sqlStr) {
        String[] years = new String[2], months = new String[2], days = new String[2], hours = new String[2];

        DecimalFormat mFormat= new DecimalFormat("00");
        mFormat.setRoundingMode(RoundingMode.DOWN);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR_OF_DAY, -1);
        years[0] = String.valueOf(cal.get(Calendar.YEAR));
        months[0] = mFormat.format(cal.get(Calendar.MONTH)+1);
        days[0] = mFormat.format(cal.get(Calendar.DAY_OF_MONTH));
        hours[0] = mFormat.format(cal.get(Calendar.HOUR_OF_DAY));

        cal.add(Calendar.HOUR_OF_DAY, -1);
        years[1] = String.valueOf(cal.get(Calendar.YEAR));
        months[1] = mFormat.format(cal.get(Calendar.MONTH)+1);
        days[1] = mFormat.format(cal.get(Calendar.DAY_OF_MONTH));
        hours[1] = mFormat.format(cal.get(Calendar.HOUR_OF_DAY));

        return sqlStr.replaceAll(SQL_STATEMENT_PLACE_HOLDER_YEAR, years[0]+" , "+years[1])
                .replaceAll(SQL_STATEMENT_PLACE_HOLDER_MONTH, months[0]+" , "+months[1])
                .replaceAll(SQL_STATEMENT_PLACE_HOLDER_DAY, days[0]+" , "+days[1])
                .replaceAll(SQL_STATEMENT_PLACE_HOLDER_HOUR, hours[0]+" , "+hours[1]);
    }

    public static String FillCreationTimeStampsInSqlStatement(String sqlStr) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("America/New_York"));

        Calendar estCalendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
        estCalendar.add(Calendar.HOUR_OF_DAY,-1);

        estCalendar.set(Calendar.MINUTE, 00);
        estCalendar.set(Calendar.SECOND, 00);
        estCalendar.set(Calendar.MILLISECOND, 000);
        String fromDateStr = dateFormat.format(estCalendar.getTime());

        estCalendar.set(Calendar.MINUTE, 59);
        estCalendar.set(Calendar.SECOND, 59);
        estCalendar.set(Calendar.MILLISECOND, 999);
        String toDateStr = dateFormat.format(estCalendar.getTime());

        return sqlStr.replaceAll(SQL_STATEMENT_PLACE_HOLDER_FROM_TIME, fromDateStr)
                .replaceAll(SQL_STATEMENT_PLACE_HOLDER_TO_TIME, toDateStr);
    }

    public static List<IncomingBankConnectionEvent> ParseResultSetForBankConnectionEvents(
            ResultSet resultSet) throws SQLException {
        List<IncomingBankConnectionEvent> eventList = new ArrayList<IncomingBankConnectionEvent>();
        while (resultSet.next()) {
            IncomingBankConnectionEvent event = new IncomingBankConnectionEvent();
            event.ConnectionType = resultSet.getInt(
                    IncomingBankConnectionEvent.COLUMN_NAME_CONNECTION_TYPE);
            event.ConnectionState = resultSet.getString(
                    IncomingBankConnectionEvent.COLUMN_NAME_CONNECTION_STATE);
            event.BankFeedProvider = resultSet.getInt(
                    IncomingBankConnectionEvent.COLUMN_NAME_BANKFEED_PROVIDER);
            event.BfpProvidedBankConnectionId = resultSet.getLong(
                    IncomingBankConnectionEvent.COLUMN_NAME_BFP_PROVIDED_BANK_CONNECTION_ID);
            event.ErrorCode = resultSet.getString(
                    IncomingBankConnectionEvent.COLUMN_NAME_ERROR_CODE);
            event.ErrorDescription = resultSet.getString(
                    IncomingBankConnectionEvent.COLUMN_NAME_ERROR_DESCRIPTION);
            event.FinancialInstitutionId = resultSet.getInt(
                    IncomingBankConnectionEvent.COLUMN_NAME_FINANCIAL_INSTITUTION_ID);
            event.FinancialInstitutionName = resultSet.getString(
                    IncomingBankConnectionEvent.COLUMN_NAME_FINANCIAL_INSTITUTION_NAME);
            event.HttpStatusCode = resultSet.getInt(
                    IncomingBankConnectionEvent.COLUMN_NAME_HTTP_STATUS_CODE);
            //event.IsAuthEnabled = resultSet.getBoolean(IncomingBankConnectionEvent.COLUMN_NAME_IS_AUTH_ENABLED);
            event.RequestTime = resultSet.getLong(
                    IncomingBankConnectionEvent.COLUMN_NAME_REQUEST_TIME);
            event.EventCreationTime = resultSet.getTimestamp(IncomingBankConnectionEvent.COLUMN_NAME_EVENT_CREATION_TIME);
            event.Status = resultSet.getInt(
                    IncomingBankConnectionEvent.COLUMN_NAME_STATUS);
            event.UserId = resultSet.getLong(
                    IncomingBankConnectionEvent.COLUMN_NAME_USERID);
            event.UserProvidedBankConnectionId = resultSet.getLong(
                    IncomingBankConnectionEvent.COLUMN_NAME_USER_PROVIDED_BANK_CONNECTION_ID);

            eventList.add(event);
        }
        return eventList;
    }
}
