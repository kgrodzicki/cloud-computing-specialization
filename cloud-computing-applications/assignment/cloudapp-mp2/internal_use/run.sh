#!/bin/bash

run_assignment() {
	echo "${yellow}	Compile the Code${reset}"
	cp $1.java $PREFIX/
	rm -rf $PREFIX/build
	mkdir -p $PREFIX/build
	hadoop com.sun.tools.javac.Main $PREFIX/$1.java -d $PREFIX/build 
	jar -cvf $PREFIX/$1.jar -C $PREFIX/build/ . 

	echo "${yellow}	Run${reset}"
	hadoop jar $PREFIX/$1.jar $1 $2 $HDFS_HOME/$1-output 

	echo "${yellow}	Collect the Output${reset}"
	hadoop fs -cat $HDFS_HOME/$1-output/* > $PREFIX/output-$1.txt

	echo "${yellow}	House Keeping${reset}"
	hadoop fs -rm -r -f $HDFS_HOME/$1-output/* 

	echo "${yellow}	Post Processing${reset}"
	sort $3 $PREFIX/output-$1.txt -o $PREFIX/output-tmp.txt
	head -n 100 $PREFIX/output-tmp.txt > $PREFIX/$1.output
 	rm $PREFIX/output-tmp.txt

	md5sum $PREFIX/$1.output  > $PREFIX/$1.hash
# 	md5sum $PREFIX/output-TitleCount.txt | awk '{ print $1 }' >> $PREFIX/results.txt
}

echo "${green}Running Assingment A: Title Count${reset}"
run_assignment TitleCount "-D stopwords=$HDFS_HOME/misc/stopwords.txt -D delimiters=$HDFS_HOME/misc/delimiters.txt  $HDFS_HOME/titles" "-n -k2 -r"

echo "${green}Running Assingment B: Top Titles${reset}"
run_assignment TopTitles "-D stopwords=$HDFS_HOME/misc/stopwords.txt -D delimiters=$HDFS_HOME/misc/delimiters.txt -D N=$DATASET_N  $HDFS_HOME/titles" "-n -k2 -r"

echo "${green}Running Assingment C: Top Title Statistics${reset}"
run_assignment TopTitleStatistics "-D stopwords=$HDFS_HOME/misc/stopwords.txt -D delimiters=$HDFS_HOME/misc/delimiters.txt -D N=$DATASET_N $HDFS_HOME/titles" "-k1"

echo "${green}Running Assingment D: Orphan Pages${reset}"
run_assignment OrphanPages "$HDFS_HOME/links"  "-n -k1"

echo "${green}Running Assingment E: Top Popular Links${reset}"
run_assignment TopPopularLinks "-D N=$DATASET_N $HDFS_HOME/links"  "-n -k2 -r"

echo "${green}Running Assingment F: Popularity League${reset}"
run_assignment PopularityLeague "-D league=$HDFS_HOME/misc/league.txt  $HDFS_HOME/links"  "-n -k2 -r"


