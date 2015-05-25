package com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency;

import java.util.HashMap;

public enum Currency {
  RUPEES,
  DOLLARS,
  EUROS,
  POUNDS,
  UNKNOWN;

  private static final Currency baseCurrency;
  private static final HashMap<Currency,Double> conversions;

  static {
    baseCurrency = DOLLARS;

    conversions = new HashMap<Currency, Double>();
    conversions.put(DOLLARS,1.0);
    conversions.put(EUROS,0.910);
    conversions.put(POUNDS,0.650);
    conversions.put(RUPEES,63.490);
    conversions.put(UNKNOWN,1.0);
  }

  public Double getConversionFactor(Currency currency) {
    if(this.equals(DOLLARS))
      return conversions.get(currency);

    return Math.round(conversions.get(currency)/conversions.get(this)*1000)/1000.0;
  }

  public Boolean isUnknown() {
    return this.equals(UNKNOWN);
  }
}
