package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.youngVehicleData;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.KEY_SEPARATOR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.VEHICLE;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy.TaggedVehicleMapper.VEHICLE_COLUMN_SEPARATOR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy.TaggedVehicleMapper.VEHICLE_DATE_FORMAT;

public class TaggedVehicleMapperTest {
  MapDriver<Object, Text, Text, Text> mapDriver;

  @Before
  public void setup() throws IOException {
    TaggedVehicleMapper taggedRepairMapper = new TaggedVehicleMapper();

    mapDriver = MapDriver
      .newMapDriver(taggedRepairMapper);

    Configuration driverConfiguration = mapDriver.getConfiguration();
    driverConfiguration.set(VEHICLE_COLUMN_SEPARATOR, ",");
    driverConfiguration.set(VEHICLE_DATE_FORMAT, "yyyy-MM-dd");
  }

  @Test
  public void shouldTagARepairWithItsCorrespondingTag() throws IOException {
    VehicleData vehicleData = youngVehicleData().build();
    Vehicle vehicle = new Vehicle(vehicleData);
    Text vehicleText = VehicleDataBuilders.toText(vehicleData, ",");

    mapDriver
      .withInput(NullWritable.get(), vehicleText)
      .withOutput(new Text(vehicle.getType()), new Text(vehicleText + KEY_SEPARATOR + VEHICLE));

    mapDriver.runTest();
  }

}