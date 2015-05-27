package com.thoughtworks.yottabyte.datamodels;

import org.junit.Test;

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataMaker.dummyRepair;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RepairDataTest {

  @Test
  public void shouldParseFromRowGivenAParsingRegex(){
    String repairRow = "car;c102;repair broken windows;rupees;10000";
    RepairData repairData = new RepairData(repairRow,";");

    assertThat(repairData.getVehicleType(),is("car"));
    assertThat(repairData.getCode(),is("c102"));
    assertThat(repairData.getDescription(),is("repair broken windows"));
    assertThat(repairData.getCurrency(),is("RUPEES"));
    assertThat(repairData.getAmount(),is(10000.0));
  }

  @Test
  public void shouldConvertToStringRepresentation(){
    RepairData repairData = make(a(dummyRepair));
    assertThat(repairData.toStringRepresentation("|"),
      is("dummy vehicle|dummy code|dummy description|some currency|0.0"));
  }

}
