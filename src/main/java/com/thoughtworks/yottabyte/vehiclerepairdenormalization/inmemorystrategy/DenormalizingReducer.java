package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

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

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.KEY_SEPARATOR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.valueOf;

public class DenormalizingReducer extends Reducer<Text, Text, NullWritable, Text> {

  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";
  public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";

  private Configuration configuration;

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    List<Repair> repairs = new ArrayList<>();
    List<Vehicle> vehicles = new ArrayList<>();

    populateRepairsAndVehicles(values, repairs, vehicles);
    joinVehicleWithRepair(context, repairs, vehicles);

  }

  protected void joinVehicleWithRepair(Context context, List<Repair> repairs, List<Vehicle> vehicles)
    throws IOException, InterruptedException {
    for (Vehicle vehicle : vehicles) {
      for (Repair repair : repairs) {
        context.write(NullWritable.get(), new Text(vehicle.getRegistrationNumber() + "," + repair.getCode()));
      }
    }
  }

  protected void populateRepairsAndVehicles(Iterable<Text> values, List<Repair> repairs, List<Vehicle> vehicles) {
    for (Text value : values) {
      String[] splits = value.toString().split(KEY_SEPARATOR);
      String row = splits[0];
      Tag tag = valueOf(splits[1]);
      switch (tag) {
        case REPAIR:
          repairs.add(new Repair(new RepairData(row, get(REPAIR_COLUMN_SEPARATOR))));
          break;
        case VEHICLE:
          vehicles.add(new Vehicle(new VehicleData(row, get(VEHICLE_COLUMN_SEPARATOR),
            get(VEHICLE_DATE_FORMAT))));
          break;
      }
    }
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

}

