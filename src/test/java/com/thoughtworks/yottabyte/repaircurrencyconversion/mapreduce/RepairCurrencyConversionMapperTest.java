package com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.natpryce.makeiteasy.MakeItEasy.*;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataMaker.*;

public class RepairCurrencyConversionMapperTest {
  MapDriver<Object,Text,NullWritable,Text> mapDriver;

  @Before
  public void setup() throws IOException {
    Mapper<Object, Text, NullWritable, Text> repairMapper = new RepairCurrencyConversionMapper();

    mapDriver = MapDriver
      .newMapDriver(repairMapper);

    Configuration driverConfiguration = mapDriver.getConfiguration();
    driverConfiguration.set(RepairCurrencyConversionMapper.COLUMN_SEPARATOR,",");

  }

  @Test
  public void shouldNotChangeTheCostOfARepairAlreadyInDollars() throws IOException {

    Text repairInDollars = toText(make(a(dummyDollarRepair, with(cost,1.0))),",");
    Text expectedRepairInDollars = repairInDollars;

    mapDriver
      .withInput(NullWritable.get(), repairInDollars)
      .withOutput(NullWritable.get(), expectedRepairInDollars);
    mapDriver.runTest();
  }

  @Test
  public void shouldChangeTheCostOfARepairToDollarWhenInDifferentCurrency() throws IOException {
    Text repairInRupees = toText(make(a(dummyRupeeRepair, with(cost,120.0))),",");
    Text repairInDollars = toText(make(a(dummyDollarRepair, with(cost,1.92))),",");

    mapDriver
      .withInput(NullWritable.get(), repairInRupees)
      .withOutput(NullWritable.get(), repairInDollars);
    mapDriver.runTest();
  }

}
