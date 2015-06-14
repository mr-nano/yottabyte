package com.thoughtworks.yottabyte.vehiclerepairdenormalization.mapsidestrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.RepairParser;
import com.thoughtworks.yottabyte.vehiclefiltercount.domainmodels.Vehicle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIRS;

public class DenormalizingMapper extends Mapper<Object, Text, NullWritable, Text> {

  private List<Repair> repairs = new ArrayList<>();

  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";
  public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";

  private Configuration configuration;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
    repairs = new RepairParser(get(REPAIR_COLUMN_SEPARATOR))
      .parse(new File(REPAIRS.distributedCacheLocation()));
  }

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Vehicle vehicle = new Vehicle(new VehicleData(value.toString(),
      get(VEHICLE_COLUMN_SEPARATOR), get(VEHICLE_DATE_FORMAT)));
    for (Repair repair : repairs) {
      context.write(NullWritable.get(), new Text(vehicle.getRegistrationNumber() + "," + repair.getCode()));
    }
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
