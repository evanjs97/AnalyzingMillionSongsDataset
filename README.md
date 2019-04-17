## CS455-Distributed Systems: Million Song Dataset Analysis
### Author: Evan Steiner
### Purpose: Learn about the development by analyzing the million song dataset with apache hadoop

#### Instructions, Compilation/Running:
* Compile on command line: "gradle build"
* To run jobs 1-6 and 8-9: 
	* $HADOOP_HOME/bin/hadoop jar millionSongs/build/libs/millionSongs-1.0.jar Entry -1 pathToAnalysisFiles pathToMetadataFiles outputDir
* To run job 7
	* $HADOOP_HOME/bin/hadoop jar millionSongs/build/libs/millionSongs-1.0.jar Entry -7 pathToAnalysisFiles outputDirectory	
	
#### Tasks:
* Task 1: Which artist has the most songs in the data set?
* Task 2: Which artist’s songs are the loudest on average?
* Task 3: What is the song with the highest hotttnesss (popularity) score?
* Task 4: Which artist has the highest total time spent fading in their songs?
* Task 5: What is the longest song(s)? The shortest song(s)? The song(s) of median length?
* Task 6: What are the 10 most energetic and danceable songs? List them in descending order.
* Task 7: Create segment data for the average song. Include start time, pitch, timbre, max loudness,
          max loudness time, and start loudness. 
	* Create most average song segment length data, then populate with average values for all songs segments
* Task 8: Which artist is the most generic? Which artist is the most unique?
	* Sum up lists of all similar artists, most generic is one referenced the most, most unique is artist with fewest.
* Task 9: Create a song with a higher/highest hotness score (or at least tied for highest)
	* Find all songs with very high hotness score, then average their values
* Task 10: Predicting artist based off song attributes such as tempo, key, duration, fade_in etc.
	* Using Apache Sparks MultilayerPerceptronClassifier

#### Classes/Project Structure:
* java: 
	* cs455.hadoop:
		* combiner:
			* SixTaskCombiner.java: Combiner for tasks 1-6 and 8-9 (I know the name makes no sense)
			* TaskSevenCombiner.java: Combiner for task 7
		* mapper:
			* SixTaskAnalysisMapper.java: Mapper for the analysis files (used for tasks 1-6 and 8-9)
			* SixTaskMetadataMapper.java: Mapper for the metadata files (used for tasks 1-6 and 8-9)
			* TaskSevenMapper.java: Mapper for task seven
		* partitioner:
			* SixTaskPartitioner.java: Partitioner for tasks 1-6 and 8-9 sends each task to seperate reducer
		* reducer:
			* SixTaskReducer.java: Reducer for tasks 1-6 and task 8-9
			* TaskSevenReducer.java: Reducer task seven
		* util:
			* pair
				* TwoPairStringIng.java: StringIntPairClass
			* AvgDouble.java: Avg Double class keeps track of total sum and number of additions to double, is a Writable class
			* MapKey.java: Used in TreeMaps to correctly sort different keys by their sums, is a Comparable class
			* SongSegment.java: Used in job seven to store all segment data in writable arrays (Using AvgDoubles), is a Writable class
			* TreeFormatter.java: Used to keep track of a tree of key/values (MapKeys), put new items in retrive items and top N items
			* Util.java: Lots of functions such as reading, converting and formatting
	Entry.java: Used as entry point to hadoop program, can run jobs 1-6 & 8-9 or job 7
* scala:
	* cs455.spark
		* CSVParser.scala: Used to parse the input analysis and metadata files into the SVM output file
		* Classifier.scala: Used to train and classify the input SVM file using MultilayerPerceptronClassifier
		* Main.scala: entry point for program used to run either the CSVParser or the Classifier