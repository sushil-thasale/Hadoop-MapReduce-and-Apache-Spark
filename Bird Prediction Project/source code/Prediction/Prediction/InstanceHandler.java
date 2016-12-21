package Prediction;

import java.util.ArrayList;
import java.util.List;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

/*
 * Class for handling WEKA instances and training sets
 */
public class InstanceHandler {
	
	public static Instances initInstances() {
		
		// creating a list of weka attributes
		// used for model training
		List<String> classAttributes = new ArrayList<String>();
		classAttributes.add("true");
		classAttributes.add("false");
		Attribute birdPresent = new Attribute("birdPresent", classAttributes);

		Attribute TIME = new Attribute("TIME");
		Attribute EFFORT_HRS = new Attribute("EFFORT_HRS");
		Attribute POPULATION_PER_SQ_MILE = new Attribute("POPULATION");
		Attribute HOUSING_DENSITY = new Attribute("HOUSING_DENSITY");
		Attribute HOUSING_VACANT = new Attribute("HOUSING_VACANT");
		Attribute OMERNIK_L3_ECOREGION = new Attribute("OMERNIK_L3_ECOREGION");
		Attribute TEMP_AVG = new Attribute("TEMP_AVG");
		Attribute FLOWING_FRESH_IN = new Attribute("FLOWING_FRESH_IN");
		Attribute WETVEG_FRESH_FROM = new Attribute("WETVEG_FRESH_FROM");
		Attribute WETVEG_FRESH_IN = new Attribute("WETVEG_FRESH_IN");
		Attribute FLOWING_BRACKISH_FROM = new Attribute("FLOWING_BRACKISH_FROM");
		Attribute FLOWING_BRACKISH_IN = new Attribute("FLOWING_BRACKISH_IN");
		Attribute STANDING_BRACKISH_FROM = new Attribute(
				"STANDING_BRACKISH_FROM");
		Attribute STANDING_BRACKISH_IN = new Attribute("STANDING_BRACKISH_IN");
		Attribute WETVEG_BRACKISH_FROM = new Attribute("WETVEG_BRACKISH_FROM");
		Attribute WETVEG_BRACKISH_IN = new Attribute("WETVEG_BRACKISH_IN");
		Attribute MONTH = new Attribute("MONTH");

	

		// Declare the feature vector
		ArrayList<Attribute> wekaAttributes = new ArrayList<Attribute>();
		wekaAttributes.add(birdPresent);
		wekaAttributes.add(TIME);
		wekaAttributes.add(EFFORT_HRS);
		wekaAttributes.add(POPULATION_PER_SQ_MILE);
		wekaAttributes.add(HOUSING_DENSITY);
		wekaAttributes.add(HOUSING_VACANT);
		wekaAttributes.add(OMERNIK_L3_ECOREGION);
		wekaAttributes.add(TEMP_AVG);
		wekaAttributes.add(FLOWING_FRESH_IN);
		wekaAttributes.add(WETVEG_FRESH_FROM);
		wekaAttributes.add(WETVEG_FRESH_IN);
		wekaAttributes.add(FLOWING_BRACKISH_FROM);
		wekaAttributes.add(FLOWING_BRACKISH_IN);
		wekaAttributes.add(STANDING_BRACKISH_FROM);
		wekaAttributes.add(STANDING_BRACKISH_IN);
		wekaAttributes.add(WETVEG_BRACKISH_FROM);
		wekaAttributes.add(WETVEG_BRACKISH_IN);
		wekaAttributes.add(MONTH);		


		Instances set = new Instances("Model", wekaAttributes, 0);
		// index that needs to be classified
		set.setClassIndex(0);

		return set;
	}
	
	/*
	 * Builds instance as the given sampling data
	 * If any column has as outlier "-999", set it as a missing attribute
	 */
	public static Instance getInstance(SamplingEventDetails samplingEventDetails,
			Instances set) {
		int i =0;
		
		Instance instance = new DenseInstance(18);

		instance.setValue(set.attribute(i++),
				samplingEventDetails.birdPresent.get()+"");
		
		instance.setValue(set.attribute(i++),
				samplingEventDetails.TIME.get());

		if (samplingEventDetails.EFFORT_HRS.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.EFFORT_HRS.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.POPULATION_PER_MILE.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.POPULATION_PER_MILE.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.HOUSING_DENSITY.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.HOUSING_DENSITY.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.HOUSING_VACANT.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.HOUSING_VACANT.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.OMERNIK_L3_ECOREGION.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.OMERNIK_L3_ECOREGION.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.TEMP_AVG.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.TEMP_AVG.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.FLOWING_FRESH_IN.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.FLOWING_FRESH_IN.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.WETVEG_FRESH_FROM.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.WETVEG_FRESH_FROM.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.WETVEG_FRESH_IN.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.WETVEG_FRESH_IN.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.FLOWING_BRACKISH_FROM.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.FLOWING_BRACKISH_FROM.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.FLOWING_BRACKISH_IN.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.FLOWING_BRACKISH_IN.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.STANDING_BRACKISH_FROM.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.STANDING_BRACKISH_FROM.get());
		} else
			instance.setMissing(set.attribute(i++));

		if (samplingEventDetails.STANDING_BRACKISH_IN.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.STANDING_BRACKISH_IN.get());
		} else {
			instance.setMissing(set.attribute(i++));
			// System.out.println("-999 found");
		}

		if (samplingEventDetails.WETVEG_BRACKISH_FROM.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.WETVEG_BRACKISH_FROM.get());
		} else {
			instance.setMissing(set.attribute(i++));
//			System.out.println("-999 found");
		}

		if (samplingEventDetails.WETVEG_BRACKISH_IN.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.WETVEG_BRACKISH_IN.get());
		} else {
			instance.setMissing(set.attribute(i++));
			// System.out.println("-999 found");
		}

		if (samplingEventDetails.MONTH.get() != -999) {
			instance.setValue(set.attribute(i++),
					samplingEventDetails.MONTH.get());
		} else
			instance.setMissing(set.attribute(i++));

//		set.add(instance);
		return instance;
	}
}
