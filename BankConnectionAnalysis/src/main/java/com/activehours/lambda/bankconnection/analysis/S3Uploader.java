package com.activehours.lambda.bankconnection.analysis;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

public class S3Uploader {
    private static final int S3_OBJECTS_COUNT_TO_WRITE_TO_S3 = 2500;
    private ArrayList<Object> mS3ObjectList = new ArrayList<Object>();

    private AmazonS3 mS3Clinet;
    private LambdaLogger mLogger;
    private String mAthenaTableName;
    private String mS3BucketName;
    private Calendar mEstCalendar;
    private String mDestinationS3Folder;

    public S3Uploader(String athenaTableName, String s3BucketName, LambdaLogger logger) {
        mAthenaTableName = athenaTableName;
        mS3BucketName = s3BucketName;
        mLogger = logger;
        mS3Clinet = AmazonS3ClientBuilder.defaultClient();

        mEstCalendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
        mEstCalendar.add(Calendar.HOUR_OF_DAY,-1);
        DecimalFormat numberFormat= new DecimalFormat("00");
        numberFormat.setRoundingMode(RoundingMode.DOWN);

        mDestinationS3Folder = String.valueOf(mEstCalendar.get(Calendar.YEAR))
                +"/"
                +numberFormat.format(mEstCalendar.get(Calendar.MONTH)+1)
                +"/"
                +numberFormat.format(mEstCalendar.get(Calendar.DAY_OF_MONTH))
                +"/"
                +numberFormat.format(mEstCalendar.get(Calendar.HOUR_OF_DAY))
                +"/";
    }

    public void FinalizeAndAddPartition(Connection connection) {
        writeToS3(mS3ObjectList);
        addPartition(connection);
    }

    public void uploadEvent(Object s3Event){
        if (null != s3Event) {
            mS3ObjectList.add(s3Event);
        }

        if (mS3ObjectList. size() == S3_OBJECTS_COUNT_TO_WRITE_TO_S3) {
            mLogger.log("Dumping to S3. ObjectList size: "+ mS3ObjectList.size());
            writeToS3(mS3ObjectList);
            mS3ObjectList.clear();
        }
    }

    private void addPartition(Connection connection) {
        try {
            AthenaUtils.addPartitionToAthenaTables(connection, mAthenaTableName,
                    mS3BucketName, mEstCalendar, mLogger);
        } catch (SQLException e) {
            mLogger.log("Exception while adding partition to Athena Table: "
                    +mAthenaTableName+" Exception: "+e);
        }
    }

    private void writeToS3(ArrayList<Object> s3ObjectList) {

        if (s3ObjectList.size() <= 0) {
            mLogger.log("S3Object list is empty. Hence no need to push to S3");
            return;
        }

        String key = mDestinationS3Folder + UUID.randomUUID().toString();
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ObjectMapper mapper = new ObjectMapper();
            mapper.setDateFormat(df);
            StringBuilder builder = new StringBuilder();
            for(Object s3Obj: s3ObjectList) {
                builder.append(mapper.writeValueAsString(s3Obj));
                builder.append("\n");
            }

            String s3FinalStr = builder.toString();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(s3FinalStr.length());

            // create empty content
            InputStream s3Content = new ByteArrayInputStream(s3FinalStr.getBytes());

            // create a PutObjectRequest passing the folder name suffixed by /
            PutObjectRequest putObjectRequest = new PutObjectRequest(mS3BucketName,
                    key, s3Content, metadata);

            // send request to S3 to create folder
            mS3Clinet.putObject(putObjectRequest);
            mLogger.log("Successfully uploaded to S3 File: " +key);
        }catch (Exception ex) {
            mLogger.log("Exception while uploading to S3. FileName: "+key+" Exception: "+ex);
        }
    }
}
