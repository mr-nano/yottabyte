package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.KEY_SEPARATOR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.VEHICLE;

public class TaggedVehicleMapper extends Mapper<Object,Text,Text,Text> {

  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";

  private Configuration configuration;

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
    String columnSeparator = get(VEHICLE_COLUMN_SEPARATOR);
    Vehicle vehicle = new Vehicle(new VehicleData(row.toString(),columnSeparator, ""));

    context.write(new Text(vehicle.getType()),
      new Text(row + KEY_SEPARATOR + VEHICLE));
  }

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
