package com.thoughtworks.yottabyte;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfiguredDriver extends Configured {
  private Properties properties = new Properties();

  protected void loadPropertiesFile(String propertyFilePath) throws IOException {
    try(InputStream propertiesInputStream = new FileInputStream(propertyFilePath)){
      properties.load(propertiesInputStream);
    }catch (NullPointerException npe){
      System.out.println("No properties file found");
      System.exit(1);
    }
  }

  protected String get(String propertyName){
    return Preconditions.checkNotNull(properties.getProperty(propertyName),
      "Expected %s to be present, but was not", propertyName);
  }

  protected Path getPath(String propertyName){
    return new Path(get(propertyName));
  }
}
