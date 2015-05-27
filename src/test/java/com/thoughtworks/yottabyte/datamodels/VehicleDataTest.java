package com.thoughtworks.yottabyte.datamodels;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class VehicleDataTest {

  @Test
  public void shouldParseFromRowGivenAParsingRegex(){
    String vehicleRow = "car;c10202;Someone;2005-09-11";
    VehicleData vehicleData = new VehicleData(vehicleRow,";","yyyy-MM-dd");

    assertThat(vehicleData.getType(),is("car"));
    assertThat(vehicleData.getRegistrationNumber(),is("c10202"));
    assertThat(vehicleData.getOwner(),is("Someone"));
    assertThat(vehicleData.getDate(),is(new DateTime(2005,9,11,0,0,0)));
  }

}

