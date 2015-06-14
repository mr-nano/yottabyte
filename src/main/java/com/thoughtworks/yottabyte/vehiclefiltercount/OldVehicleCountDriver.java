package com.thoughtworks.yottabyte.vehiclefiltercount;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.VEHICLES_COUNT;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.VEHICLES;
import static com.thoughtworks.yottabyte.vehiclefiltercount.OlderVehicleMapper.*;

public class OldVehicleCountDriver extends Configured implements Tool {

  private static ClassLoader loader;
  private Properties properties = new Properties();

  @Override
  public int run(String[] args) throws Exception {
    loadPropertiesFile(args[0]);
    Configuration configuration = getConf();
    configuration.set(COLUMN_SEPARATOR, get(VEHICLES.columnSeparator()));
    configuration.set(REFERENCE_DATE, get(VEHICLES.referenceDate()));
    configuration.set(REFERENCE_DATE_FORMAT, get(VEHICLES.referenceDateFormat()));
    configuration.set(VEHICLE_DATE_FORMAT, get(VEHICLES.dateFormat()));

    Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    FileInputFormat.setInputPaths(job, getPath(VEHICLES.path()));
    FileOutputFormat.setOutputPath(job, getPath(VEHICLES_COUNT.path()));

    job.setMapperClass(OlderVehicleMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(VehicleCountReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    loader = OldVehicleCountDriver.class.getClassLoader();
    if (args.length < 1) {
      args = new String[]{loader.getResource("config.properties").getPath()};
    }
    int exitCode = ToolRunner.run(new Configuration(), new OldVehicleCountDriver(), args);
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
