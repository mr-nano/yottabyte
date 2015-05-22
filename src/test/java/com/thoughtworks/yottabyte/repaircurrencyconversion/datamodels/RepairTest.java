package com.thoughtworks.yottabyte.repaircurrencyconversion.datamodels;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RepairTest {

  @Test
  public void shouldParseFromRowGivenAParsingRegex(){
    String repairRow = "car;c102;repair broken windows - rupees;10000";
    Repair repair = new Repair(repairRow,";");

    assertThat(repair.getVehicleType(),is("car"));
    assertThat(repair.getCode(),is("c102"));
    assertThat(repair.getDescription(),is("repair broken windows - rupees"));
    assertThat(repair.getCost(),is(10000.0));

  }
}
