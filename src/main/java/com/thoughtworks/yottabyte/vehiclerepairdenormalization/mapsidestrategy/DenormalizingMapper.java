package com.thoughtworks.yottabyte.vehiclerepairdenormalization.mapsidestrategy;

import com.thoughtworks.yottabyte.ConfiguredMapper;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.RepairParser;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DOLLARS;

public class DenormalizingMapper extends ConfiguredMapper<Object, Text, NullWritable, Text> {

  private List<Repair> repairs = new ArrayList<>();

  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";
  private String columnSeparator;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    repairs = new RepairParser(get(REPAIR_COLUMN_SEPARATOR))
      .parse(new File(REPAIR_IN_DOLLARS.distributedCacheLocation()));
  }

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Vehicle vehicle = new Vehicle(new VehicleData(value.toString(),get(VEHICLE_COLUMN_SEPARATOR), ""));
    for (Repair repair : repairs) {
      context.write(NullWritable.get(),        new Text(vehicle.getRegistrationNumber() + "," + repair.getCode()));
    }
  }
}
