package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataBuilders;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataBuilders.dummyDollarRepair;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.KEY_SEPARATOR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.REPAIR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy.TaggedRepairMapper.REPAIR_COLUMN_SEPARATOR;

public class TaggedRepairMapperTest {
  MapDriver<Object, Text, Text, Text> mapDriver;

  @Before
  public void setup() throws IOException {
    TaggedRepairMapper taggedRepairMapper = new TaggedRepairMapper();

    mapDriver = MapDriver
      .newMapDriver(taggedRepairMapper);

    Configuration driverConfiguration = mapDriver.getConfiguration();
    driverConfiguration.set(REPAIR_COLUMN_SEPARATOR, ",");
  }

  @Test
  public void shouldTagARepairWithItsCorrespondingTag() throws IOException {
    RepairData aRepair = dummyDollarRepair().build();
    Text aRepairText = RepairDataBuilders.toText(aRepair, ",");

    mapDriver
      .withInput(NullWritable.get(), aRepairText)
      .withOutput(new Text(aRepair.getVehicleType()),new Text(aRepairText + KEY_SEPARATOR + REPAIR));

    mapDriver.runTest();
  }

}