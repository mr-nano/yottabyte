package com.thoughtworks.yottabyte;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.junit.After;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class DriverTestBase {
  protected Configuration configuration;
  protected Properties properties;

  @After
  public void cleanup() throws IOException {
    cleanOutputDirectory();
  }

  protected void cleanOutputDirectory() throws IOException {
    FileSystem fileSystem = FileSystem.getLocal(configuration);
    for (String outputDirectory : getOutputDirectories()) {
      fileSystem.delete(new Path(outputDirectory), true);
    }
  }

  protected File makeTemporaryPropertiesFile() throws IOException {
    File tempPropertiesFile = File.createTempFile("properties", "config");
    try (FileOutputStream fos = new FileOutputStream(tempPropertiesFile)) {
      properties.store(fos, null);
    }
    return tempPropertiesFile;
  }

  protected void runTest() throws Exception {
    getDriver().setConf(configuration);
    getDriver().run(new String[]{makeTemporaryPropertiesFile().getAbsolutePath()});
  }

  protected void runTestWithInput(List<File> files) throws Exception {
    addInputFilesToProperties(files);
    runTest();
  }

  protected void runTestWithInput(File files) throws Exception {
    addInputFilesToProperties(Arrays.asList(files));
    runTest();
  }

  protected abstract List<String> getOutputDirectories();

  protected abstract Tool getDriver();

  protected abstract void addInputFilesToProperties(List<File> files);
}
