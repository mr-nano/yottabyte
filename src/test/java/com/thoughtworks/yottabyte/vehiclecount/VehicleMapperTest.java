package com.thoughtworks.yottabyte.vehiclecount;

import com.thoughtworks.yottabyte.vehiclefiltercount.domainmodels.Vehicle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.*;
import static com.thoughtworks.yottabyte.vehiclefiltercount.OlderVehicleMapper.*;

public class VehicleMapperTest {
  MapDriver<Object, Text, Text, IntWritable> mapDriver;

  @Before
  public void setup() throws IOException {
    VehicleMapper vehicleMapper = new VehicleMapper();

    mapDriver = MapDriver
      .newMapDriver(vehicleMapper);

    Configuration driverConfiguration = mapDriver.getConfiguration();
    driverConfiguration.set(COLUMN_SEPARATOR, ",");
    driverConfiguration.set(VEHICLE_DATE_FORMAT, "yyyy-MM-dd");
  }

  @Test
  public void shouldEmitAVehicleRecord() throws IOException {
    Text aVehicleText = toText(someVehicleData().build(), ",");
    Vehicle outputVehicle = new Vehicle(someVehicleData().build());

    mapDriver
      .withInput(NullWritable.get(), aVehicleText)
      .withOutput(new Text(outputVehicle.getType()),new IntWritable(1));

    mapDriver.runTest();
  }

}