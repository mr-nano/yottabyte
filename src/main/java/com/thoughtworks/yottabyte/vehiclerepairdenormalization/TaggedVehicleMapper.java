package com.thoughtworks.yottabyte.vehiclerepairdenormalization;

import com.thoughtworks.yottabyte.ConfiguredMapper;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.TaggedText;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.RecordType.VEHICLE;

public class TaggedVehicleMapper extends ConfiguredMapper<Object,Text,Text,TaggedText> {

  public static final String COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
    String columnSeparator = get(COLUMN_SEPARATOR);
    Vehicle vehicle = new Vehicle(new VehicleData(row.toString(),columnSeparator, ""));

    context.write(new Text(vehicle.getType()),
      new TaggedText(row, VEHICLE));
  }

}
