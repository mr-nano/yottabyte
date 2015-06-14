package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.vehiclefiltercount.OldVehicleCountDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.*;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy.DenormalizingReducer.*;
import static org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath;

public class Driver extends Configured implements Tool {

  private static ClassLoader loader;
  private Properties properties = new Properties();

  @Override
  public int run(String[] args) throws Exception {

    loadPropertiesFile(args[0]);
    Configuration configuration = getConf();
    configuration.set(VEHICLE_COLUMN_SEPARATOR, get(VEHICLES.columnSeparator()));
    configuration.set(REPAIR_COLUMN_SEPARATOR, get(REPAIRS.columnSeparator()));
    configuration.set(VEHICLE_DATE_FORMAT, get(VEHICLES.dateFormat()));

    Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    addInputPath(job, getPath(VEHICLES.path()), TextInputFormat.class, TaggedVehicleMapper.class);
    addInputPath(job, getPath(REPAIRS.path()), TextInputFormat.class, TaggedRepairMapper.class);

    FileOutputFormat.setOutputPath(job, getPath(VEHICLES_REPAIRS.path()));

    job.setMapOutputKeyClass(TaggedKey.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(DenormalizingReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.setSortComparatorClass(SortComparator.class);
    job.setGroupingComparatorClass(GroupingComparator.class);
    job.setPartitionerClass(Partitioner.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    loader = Driver.class.getClassLoader();
    if (args.length < 1) {
      args = new String[]{loader.getResource("config.properties").getPath()};
    }
    int exitCode = ToolRunner.run(new Configuration(), new Driver(), args);
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
