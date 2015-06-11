package com.thoughtworks.yottabyte;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class FileParser<T> {
  public List<T> parse(File vehicleFile) throws IOException {
    List<T> ts = new ArrayList<>();
    String record;
    try(BufferedReader bufferedReader = new BufferedReader(new FileReader(vehicleFile))){
      while ((record = bufferedReader.readLine()) != null){
        ts.add(parse(record));
      }
    }
    return ts;
  }

  public abstract T parse(String record);
}
