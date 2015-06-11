package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.*;

public class DenormalizingReducer extends Reducer<Text, Text, NullWritable, Text> {

  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";
  public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";
  protected List<Repair> repairs;

  private Configuration configuration;

  @Override
  protected void reduce(Text key, Iterable<Text> rows, Context context) throws IOException, InterruptedException {

    repairs = new ArrayList<>();
    Text taggedText;
    String row = null;
    Tag tag = null;

    while (rows.iterator().hasNext()) {
      taggedText = rows.iterator().next();
      String[] splits = taggedText.toString().split(KEY_SEPARATOR);
      row = splits[0];
      tag = valueOf(splits[1]);
      if (tag.equals(VEHICLE)) {
        break;
      }
      repairs.add(new Repair(new RepairData(row, get(REPAIR_COLUMN_SEPARATOR))));
    }

    if (!tag.equals(VEHICLE)) {
      return;
    }

    Vehicle vehicle = new Vehicle(new VehicleData(row,
      get(VEHICLE_COLUMN_SEPARATOR), get(VEHICLE_DATE_FORMAT)));

    join(vehicle, context);

    for (Text remainingVehicles : rows) {
      vehicle = new Vehicle(new VehicleData(remainingVehicles.toString(),
        get(VEHICLE_COLUMN_SEPARATOR), get(VEHICLE_DATE_FORMAT)));
      join(vehicle, context);
    }

  }

  private void join(Vehicle vehicle, Context context) throws IOException, InterruptedException {
    for (Repair repair : repairs) {
      context.write(NullWritable.get(), new Text(vehicle.getRegistrationNumber() + "," + repair.getCode()));
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }


  protected String get(String key) {
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }

}
