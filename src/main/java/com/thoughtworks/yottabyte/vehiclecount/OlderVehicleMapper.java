package com.thoughtworks.yottabyte.vehiclecount;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;

import static org.joda.time.Years.years;

public class OlderVehicleMapper extends Mapper<Object, Text, Text, IntWritable> {

  public static final String COLUMN_SEPARATOR = "COLUMN_SEPARATOR";
  public static final String REFERENCE_DATE = "REFERENCE_DATE";
  private Configuration configuration;
  private static final IntWritable one = new IntWritable(1);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }

  @Override
  public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
    String columnSeparator = get(COLUMN_SEPARATOR);
    DateTime referenceDate = getDate(REFERENCE_DATE);
    Vehicle vehicle = new Vehicle(new VehicleData(row.toString(),columnSeparator, ""));
    if(vehicle.isOlderThanYears(referenceDate, years(15))){
      context.write(new Text(vehicle.getType()), one);
    }
  }

  private String get(String key){
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }

  private DateTime getDate(String key){
    String dateTime =  Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
    String dateTimeFormat = Preconditions.checkNotNull(configuration.get(key + "_FORMAT"));
    return DateTimeFormat.forPattern(dateTimeFormat).parseDateTime(dateTime);
  }
}
