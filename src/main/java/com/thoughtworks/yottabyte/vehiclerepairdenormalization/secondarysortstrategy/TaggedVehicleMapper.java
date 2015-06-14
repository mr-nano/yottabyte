package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.vehiclefiltercount.domainmodels.Vehicle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.KEY_SEPARATOR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.VEHICLE;

public class TaggedVehicleMapper extends Mapper<Object,Text,TaggedKey,Text> {

  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";
  public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";

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

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
    String columnSeparator = get(VEHICLE_COLUMN_SEPARATOR);

    Vehicle vehicle = new Vehicle(new VehicleData(row.toString(),columnSeparator,
      get(VEHICLE_DATE_FORMAT)));

    context.write(new TaggedKey(vehicle.getType().trim().toUpperCase(),VEHICLE),
      new Text(row + KEY_SEPARATOR + VEHICLE));
  }

}
