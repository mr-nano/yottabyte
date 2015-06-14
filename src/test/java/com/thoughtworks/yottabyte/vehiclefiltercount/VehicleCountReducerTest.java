package com.thoughtworks.yottabyte.vehiclefiltercount;

import com.thoughtworks.yottabyte.vehiclefiltercount.domainmodels.Vehicle;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.*;
import static java.util.Collections.singletonList;

public class VehicleCountReducerTest {
  ReduceDriver<Text,IntWritable,NullWritable,Text> reduceDriver;

  @Before
  public void setup(){
    VehicleCountReducer reducer = new VehicleCountReducer();

    reduceDriver = ReduceDriver
      .newReduceDriver(reducer);
  }

  @Test
  public void shouldSumUpTotalNumberOfVehiclesForOneType(){
    Vehicle anOldVehicle = new Vehicle(veryOldVehicleData().build());

    reduceDriver
      .withInput(new Text(anOldVehicle.getType()), singletonList(new IntWritable(1)))
      .withOutput(NullWritable.get(),new Text("Type " + anOldVehicle.getType() + " found 1 times."));
  }


}