# Hadoop-Basic-Programs
Basic Hadoop Programs

## Changes Todo
- Change the path over here:
```java
FileInputFormat.addInputPath(job, new Path("input.txt"));
FileOutputFormat.setOutputPath(job, new Path("output"));
````
- Change the output path object for every program:
```java
 FileOutputFormat.setOutputPath(job, new Path("out45" ));
 FileInputFormat.addInputPath(job1, new Path("out45"));
```
## MapReduce programs on files containing text 

- [Find Frequency of each word in the file](/NStacks.java)
- [Find the word with the highest frequency from the input file](/WordHighFreq.java)
- [Find occurance of a search query in the text file](/WordSearch.java)

## MapReduce programs on files containing numbers

- [Find the largest integer from the file](/LargestNum.java)
- [Find the average of all the integers from the input file](/AverageNum.java)
- [Find count of the number of distinct integers from the text file](/Unique.java)
- [Find set of odd and even numbers from the text file](/OddEven.java)
