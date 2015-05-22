package com.thoughtworks.yottabyte.repaircurrencyconversion.datamodels;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RepairDataTest {

  @Test
  public void shouldParseFromRowGivenAParsingRegex(){
    String repairRow = "car;c102;repairData broken windows - rupees;10000";
    RepairData repairData = new RepairData(repairRow,";");

    assertThat(repairData.getVehicleType(),is("car"));
    assertThat(repairData.getCode(),is("c102"));
    assertThat(repairData.getDescription(),is("repairData broken windows - rupees"));
    assertThat(repairData.getCost(),is(10000.0));

  }
}
