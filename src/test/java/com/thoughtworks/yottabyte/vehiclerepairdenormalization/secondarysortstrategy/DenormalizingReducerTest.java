package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.vehiclefiltercount.domainmodels.Vehicle;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static com.google.common.base.Joiner.on;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataBuilders.dummyRepair;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.someVehicleData;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.*;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy.DenormalizingReducer.*;

public class DenormalizingReducerTest {
  ReduceDriver<TaggedKey,Text,NullWritable,Text> reduceDriver;

  @Before
  public void setup(){
    DenormalizingReducer reducer = new DenormalizingReducer();

    reduceDriver = ReduceDriver
      .newReduceDriver(reducer);

    Configuration driverConfiguration = reduceDriver.getConfiguration();
    driverConfiguration.set(REPAIR_COLUMN_SEPARATOR, ",");
    driverConfiguration.set(VEHICLE_COLUMN_SEPARATOR, ",");
    driverConfiguration.set(VEHICLE_DATE_FORMAT, "yyyy-MM-dd");
  }

  @Test
  public void shouldNotEmitAnyRecordsIfThereAreNoVehicles() throws IOException {

    Repair repairOne = new Repair(dummyRepair().vehicleType("car").build());
    Text repairOneText = new Text(repairOne.toStringRepresentation(",") + KEY_SEPARATOR + REPAIR);

    Repair repairTwo = new Repair(dummyRepair().vehicleType("car").build());
    Text repairTwoText = new Text(repairTwo.toStringRepresentation(",") + KEY_SEPARATOR + REPAIR);

    reduceDriver
      .withInput(new TaggedKey("car", Tag.REPAIR), Arrays.asList(repairOneText,repairTwoText))
      .runTest();
  }

  @Test
  public void shouldNotEmitAnyRecordsIfThereAreNoRepairs() throws IOException {

    Vehicle vehicle = new Vehicle(someVehicleData().type("car").build());
    Text vehicleText = new Text(vehicle.toStringRepresentation(",") + KEY_SEPARATOR + VEHICLE);

    reduceDriver
      .withInput(new TaggedKey("car", Tag.VEHICLE), Arrays.asList(vehicleText))
      .runTest();
  }

  @Test
  public void shouldJoinVehiclesWithRepairsGivenSortedOrder() throws IOException {

    Vehicle vehicle = new Vehicle(someVehicleData().type("car").build());
    Text vehicleText = new Text(vehicle.toStringRepresentation(",") + KEY_SEPARATOR + VEHICLE);

    Repair repair = new Repair(dummyRepair().vehicleType("car").build());
    Text repairText = new Text(repair.toStringRepresentation(",") + KEY_SEPARATOR + REPAIR);

    reduceDriver
      .withInput(new TaggedKey("car", Tag.REPAIR), Arrays.asList(repairText,vehicleText))
      .withOutput(NullWritable.get(),new Text(on(",").join(vehicle.getType(), vehicle.getRegistrationNumber(),
        repair.getCode(), repair.getDescription())))
      .runTest();
  }

}