package com.activehours.lambda.bankconnection.analysis.Model.firehose;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class FirehoseRecord {
	public FirehoseRecord() {};
	
    public String recordId;

    public String approximateArrivalTimestamp;

    public String result;

    public String data;

    public String DecodeData()
    {
        return new String(Base64.getDecoder().decode(this.data), StandardCharsets.UTF_8);
    }
}
