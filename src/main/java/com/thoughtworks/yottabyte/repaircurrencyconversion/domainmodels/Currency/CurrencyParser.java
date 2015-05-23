package com.thoughtworks.yottabyte.repaircurrencyconversion.domainmodels.Currency;

import java.util.HashMap;

public class CurrencyParser {

  private static final HashMap<String, String> currencyStringMapping;

  static {
    currencyStringMapping = new HashMap<String, String>();
    currencyStringMapping.put("RUPEE", "RUPEES");
    currencyStringMapping.put("EURO", "EUROS");
    currencyStringMapping.put("POUND", "POUNDS");
    currencyStringMapping.put("DOLLAR", "DOLLARS");
    currencyStringMapping.put("RUPEES", "RUPEES");
    currencyStringMapping.put("EUROS", "EUROS");
    currencyStringMapping.put("POUNDS", "POUNDS");
    currencyStringMapping.put("DOLLARS", "DOLLARS");
  }

  public static Currency fromString(String currency) {
    try {
      return Currency.valueOf(currencyStringMapping.get(currency.toUpperCase()));
    } catch (NullPointerException ex) {
      return Currency.UNKNOWN;
    }
  }
}
