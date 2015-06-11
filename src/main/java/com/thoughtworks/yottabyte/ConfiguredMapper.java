package com.thoughtworks.yottabyte;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;

public class ConfiguredMapper<K1,V1,K2,V2> extends Mapper<K1,V1,K2,V2> {

  private Configuration configuration;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }

  protected String get(String key){
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }

  protected DateTime getDate(String key){
    String dateTime =  Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
    String dateTimeFormat = Preconditions.checkNotNull(configuration.get(key + "_FORMAT"),
      "Expected %s.FORMAT to be present, but was not", key);
    return DateTimeFormat.forPattern(dateTimeFormat).parseDateTime(dateTime);
  }
}
