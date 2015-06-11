package com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DIFFERENT_CURRENCIES;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DOLLARS;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce.RepairCurrencyConversionMapper.COLUMN_SEPARATOR;

public class RepairCurrencyConversionDriver extends Configured implements Tool{

  private static ClassLoader loader;
  private Properties properties = new Properties();

  @Override
  public int run(String[] args) throws Exception {

    loadPropertiesFile(args[0]);
    Configuration configuration = getConf();
    configuration.set(COLUMN_SEPARATOR, get(REPAIR_IN_DIFFERENT_CURRENCIES.columnSeparator()));

    Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    FileInputFormat.setInputPaths(job, getPath(REPAIR_IN_DIFFERENT_CURRENCIES.path()));
    FileOutputFormat.setOutputPath(job, getPath(REPAIR_IN_DOLLARS.path()));

    job.setMapperClass(RepairCurrencyConversionMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(0);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    loader = RepairCurrencyConversionDriver.class.getClassLoader();
    if(args.length < 1){
      File tempFile = File.createTempFile("config", "properties");
      Files.copy(loader.getResourceAsStream("config.properties"), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      args = new String[]{tempFile.getPath()};
    }
    int exitCode = ToolRunner.run(new Configuration(), new RepairCurrencyConversionDriver(), args);
    System.exit(exitCode);
  }

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
