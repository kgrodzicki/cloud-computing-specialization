Group 1 (Answer any 2):

Rank the top 10 most popular airports by numbers of flights to/from the airport.
Rank the top 10 airlines by on-time arrival performance.
Rank the days of the week by on-time arrival performance.

Group 2 (Answer any 3):
Clarification 1/27/2016: For questions 1 and 2 below, we are asking you to find, for each airport, the top 10 carriers and destination airports from that airport with respect to on-time departure performance. We are not asking you to rank the overall top 10 carriers/airports. For specific queries, see the Task 1 Queries and Task 2 Queries.
For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.
For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.
For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.
For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y.

Group 3 (Answer both using Hadoop and Question 2 using Spark Streaming):
Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements (For specific queries, see the Task 1 Queries and Task 2 Queries):
The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
Tom wants to arrive at each destination with as little delay as possible (Clarification 1/24/16: assume you know the actual delay of each flight).
Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy constraints (a) and (b) and have the best individual performance with respect to constraint (c), if such flights exist.

