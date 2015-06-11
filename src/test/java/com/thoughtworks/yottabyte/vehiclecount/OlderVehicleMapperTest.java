package com.thoughtworks.yottabyte.vehiclecount;

import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.toText;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.veryOldVehicleData;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.youngVehicleData;
import static com.thoughtworks.yottabyte.vehiclecount.OlderVehicleMapper.*;

public class OlderVehicleMapperTest {
  MapDriver<Object, Text, Text, IntWritable> mapDriver;

  @Before
  public void setup() throws IOException {
    OlderVehicleMapper olderVehicleMapper = new OlderVehicleMapper();

    mapDriver = MapDriver
      .newMapDriver(olderVehicleMapper);

    Configuration driverConfiguration = mapDriver.getConfiguration();
    driverConfiguration.set(COLUMN_SEPARATOR, ",");
    driverConfiguration.set(REFERENCE_DATE, "2015-05-01");
    driverConfiguration.set(REFERENCE_DATE_FORMAT, "yyyy-MM-dd");
    driverConfiguration.set(VEHICLE_DATE_FORMAT, "yyyy-MM-dd");
  }

  @Test
  public void shouldEmitAVehicleIfItsMoreThan15YearsOld() throws IOException {
    Text anOldVehicleText = toText(veryOldVehicleData().build(), ",");
    Vehicle anOldVehicle = new Vehicle(veryOldVehicleData().build());

    mapDriver
      .withInput(NullWritable.get(), anOldVehicleText)
      .withOutput(new Text(anOldVehicle.getType()),new IntWritable(1));

    mapDriver.runTest();
  }

  @Test
  public void shouldNotEmitAVehicleIfItsYoungerThan15Years() throws IOException {
    Text aYoungVehicleText = toText(youngVehicleData().build(), ",");

    mapDriver
      .withInput(NullWritable.get(), aYoungVehicleText);

    mapDriver.runTest();
  }

}