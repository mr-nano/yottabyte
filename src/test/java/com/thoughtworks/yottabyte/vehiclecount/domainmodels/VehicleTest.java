package com.thoughtworks.yottabyte.vehiclecount.domainmodels;

import com.googlecode.zohhak.api.TestWith;
import com.googlecode.zohhak.api.runners.ZohhakRunner;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import org.joda.time.DateTime;
import org.joda.time.Years;
import org.junit.runner.RunWith;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(ZohhakRunner.class)
public class VehicleTest {

  @TestWith({
    "2011-09-11,5,true",
    "2011-09-11,10,false",
  })
  public void shouldCheckIfOlderThanCertainYears(String referenceDate, Integer years, Boolean assertion) {
    Vehicle vehicle = new Vehicle(new VehicleData("car;c10202;Someone;2005-09-11", ";", "yyyy-mm-dd"));
    assertThat(vehicle.isOlderThanYears(DateTime.parse(referenceDate), Years.years(years)), is(assertion));
  }
}