package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import org.junit.Test;

import java.util.List;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.REPAIR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.VEHICLE;
import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class TaggedKeyTest {

  @Test
  public void shouldSortFirstOnTheVehicleTypeAndThenOnTagType(){
    TaggedKey taggedKeyOne = new TaggedKey("car", VEHICLE);
    TaggedKey taggedKeyTwo = new TaggedKey("truck", VEHICLE);
    TaggedKey taggedKeyThree = new TaggedKey("car", REPAIR);
    TaggedKey taggedKeyFour = new TaggedKey("truck", REPAIR);

    List<TaggedKey> someSequence = asList(taggedKeyThree, taggedKeyOne,
      taggedKeyTwo, taggedKeyFour);
    sort(someSequence);

    assertThat(someSequence,contains(taggedKeyThree,taggedKeyOne,taggedKeyFour,taggedKeyTwo));
  }

}