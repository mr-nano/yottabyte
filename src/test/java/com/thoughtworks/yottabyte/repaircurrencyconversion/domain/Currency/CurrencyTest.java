package com.thoughtworks.yottabyte.repaircurrencyconversion.domain.currency;

import com.googlecode.zohhak.api.TestWith;
import com.googlecode.zohhak.api.runners.ZohhakRunner;
import org.junit.runner.RunWith;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.domain.currency.Currency.DOLLARS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(ZohhakRunner.class)
public class CurrencyTest {

  @TestWith({
    "DOLLARS, false",
    "RUPEES, false",
    "EUROS, false",
    "POUNDS, false",
    "UNKNOWN, true",
  })
  public void theFlagForUnknownCurrencyShouldOnlyBeTrueForUnknownCurrency(Currency actualCurrency, Boolean shouldBeUnknown){
    assertThat(actualCurrency.isUnknown(), is(shouldBeUnknown));
  }

  @TestWith({
    "DOLLARS, 1.0",
    "RUPEES, 63.49",
    "EUROS, 0.91",
    "POUNDS, 0.65",
    "UNKNOWN, 1.0",
  })
  public void shouldReturnBaseCurrencyConversionsWhenConvertingFromDollar(Currency currencyToBeConvertedTo, Double expectedConversion){
    assertEquals(expectedConversion,DOLLARS.getConversionFactor(currencyToBeConvertedTo));
  }

  @TestWith({
    "EUROS, RUPEES, 69.769",
    "RUPEES, EUROS, 0.014",
  })
  public void shouldBeAbleToConvertFromOneCurrencyToAnother(Currency from, Currency to, Double expectedConversionRate){
    assertEquals(expectedConversionRate, from.getConversionFactor(to));
  }
}
