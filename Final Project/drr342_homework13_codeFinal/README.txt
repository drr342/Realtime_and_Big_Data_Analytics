CHICAGO CRIME ANALYSIS

Alisha Nitin Sawant (ans698@nyu.edu)
Daniel Rivera Ruiz (drr342@nyu.edu)

Folders, files and descriptions:
    1. analytic
        1.1 createTables.sql - Impala query to generate the required tables to perform the analytic.
        1.2 crimeTypes.csv - Text file with types of crime (used by SortCorrelations.java).
        1.3 GenerateQuery.java - Java class that generates a file correlation.sql, which will populate the correlation tables.
        1.4 SortCorrelations.java - Java class that generates results.csv file, with correlations greater or equal than 0.8 sorted in descending order.
    2. business
        2.1 BusinessDriver.java - MR Driver to clean the business licenses dataset.
        2.2 BusinessMapper.java - Mapper to clean the business licenses dataset.
        2.3 BusinessReducer.java - Reducer to clean the business licenses dataset.
    3. crime
        3.1 CrimeDriver.java - MR Driver to clean the crime dataset.
        3.2 CrimeMapper.java - Mapper to clean the crime dataset.
        3.3 CrimeReducer.java - Reducer to clean the crime dataset.
    4. demographics
        4.1 codes.csv - Mapping between demographic factors codes and descriptions.
        4.2 DemographicsDriver.java - MR Driver to clean the demographics dataset.
        4.3 DemographicsMapper.java - Mapper to clean the demographics dataset.
        4.4 DemographicsReducer.java - Reducer to clean the demographics dataset.
        4.5 tract2ca.csv - relationship between census tracts and community areas.
    5. soda
        5.1 lib - Dependencies required to connect to the Soda API.
        5.2 Capture.PNG - Visualization of the 300 mappers required to complete the job.
        5.3 Lstops.csv - original dataset with train stops and location tags.
        5.4 soda.sh - Script to run the soda MR Job.
        5.5 SodaDriver.java - MR Driver for the soda job.
        5.6 SodaMapper.java - Mapper for the soda job.
        5.7 SodaReducer.java - Reducer for the soda job.
        5.8 SodaTraffic.java - Java class to extract community areas associated to location tags of street segments.
        5.9 SodaTrains.java - Java class to extract community areas associated to location tags of train stations.
        5.10 traffic.csv - original dataset with street segments and location tags.
    6. traffic
        6.1 traffic2ca.csv - relationship between traffic segments and community areas.
        6.2 TrafficDriver.java - MR Driver to clean the traffic dataset.
        6.3 TrafficMapper.java - Mapper to clean the traffic dataset.
        6.4 TrafficReducer.java - Reducer to clean the traffic dataset.
    7. train
        7.1 train2ca.csv - relationship between train stations' locations and community areas.
        7.2 TrainDriver.java - MR Driver to clean the train stations dataset.
        7.3 TrainMapper.java - Mapper to clean the train stations dataset.
        7.4 TrainReducer.java - Reducer to clean the train stations dataset.
    8. corr_example.xlsx - Example of the correlation tables generated with the analytic.
    9. plots.xlsx - Tables and plots used for the written report.
    10. results.csv - Final result of the analytic process: high correlation table summary.
    11. run.sh - Script to run all the program.
    12. runAnalytic.sh - Script to run only the analytic part of the code (runMR.sh must have been run previously).
    13. runMR.sh - Script to run only the cleaning part of the code (MR Jobs).

To run any of the three scripts:
    $ ./run(|Analytic|MR).sh <pathToFolderInHDFS>
    All the results of the analytic will be stored to <pathToFolderInHDFS>.

About SODA (Socrata Open Data API):
    a) Online service that provides query capabilities to extract information from huge datasets online without having to download them.
    b) It was used to convert the location information (latitude and longitude) available in some of our datasets (business, traffic, train) into Chicago community areas.
    c) The City of Chicago provides a SODA dataset with the community areas and the polygons that they define (in cartesian coordinates).
    d) The SODA API provides a query function "intersects()" and checks if they intersect (in the case of a point and a polygon, it checks if the point is within the polygon).
    e) By using this function in the WHERE clause of a query, we could easily retrieve the community areas (polygons) that intersected (contained) certain points (locations of train stations, businesses, etc.)
    f) The results obtained by connecting to the SODA API are already integrated in the project. All the files in the soda directory are included only for completion and running them again is not necessary.

Results:
    a) chicago_business - Clean buisness licenses dataset.
    b) chicago_crime - Clean crime dataset.
    c) chicago_demo - Clean demographics dataset.
    d) chicago_traffic - Clean traffic dataset.
    e) chicago_train - Clean train stations dataset.
    f) correlations - Folder containing 78 correlation tables, one per community area, plus one global for the whole city.
All these results will be generated by running the code provided. A copy is also available for reference at /user/drr342/results
    g) results.csv - Correlations with absolute value greater or equal to 0.8, sorted in decreasing order. COMMUNITY_AREA = 0 means whole city of Chicago.
This result is available in the local file system, in the same folder as this file.
