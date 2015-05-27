package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.thoughtworks.yottabyte.ConfiguredReducer;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.RecordType;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.TaggedText;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DenormalizingReducer extends ConfiguredReducer<Text, TaggedText<RecordType>, NullWritable, Text> {

  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";

  @Override
  protected void reduce(Text key, Iterable<TaggedText<RecordType>> values, Context context) throws IOException, InterruptedException {
    List<Repair> repairs = new ArrayList<>();
    List<Vehicle> vehicles = new ArrayList<>();

    for (TaggedText<RecordType> value : values) {
      switch (value.tag()) {
        case REPAIR:
          repairs.add(new Repair(new RepairData(value.row(), get(REPAIR_COLUMN_SEPARATOR))));
          break;
        case VEHICLE:
          vehicles.add(new Vehicle(new VehicleData(value.row(), get(VEHICLE_COLUMN_SEPARATOR), "")));
          break;

      }
    }

    for (Vehicle vehicle : vehicles) {
      for (Repair repair : repairs) {
        context.write(NullWritable.get(), new Text(vehicle.getRegistrationNumber() + "," + repair.getCode()));
      }
    }
  }

}

