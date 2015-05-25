package com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency;

import com.googlecode.zohhak.api.TestWith;
import com.googlecode.zohhak.api.runners.ZohhakRunner;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;

@RunWith(ZohhakRunner.class)
public class CurrencyParserTest {

  @TestWith({
    "dollars, DOLLARS",
    "dollar, DOLLARS",
    "rupees, RUPEES",
    "rupee, RUPEES",
    "euro, EUROS",
    "euros, EUROS",
    "pounds, POUNDS",
    "pound, POUNDS",
    "yen, UNKNOWN",
    ", UNKNOWN",
    "$, UNKNOWN",
    "Rs, UNKNOWN",
  })
  public void shouldParseAStringIntoCurrency(String currencyString, Currency expectedCurrency) {
    assertEquals(expectedCurrency, CurrencyParser.fromString(currencyString));
  }


}
