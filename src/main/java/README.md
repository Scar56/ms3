Summary:
This repo is for the purpose of interviewing with ms3
Requirements:
1. Java application that will consume a CSV file, parse the data, and insert valid records into a SQLite database
    a. Database name <input-filename>.db
    b. It should have 1 table with 10 columns, A, B, C, D, E, F, G, H, I, J that correspond to the CSV file column header  
       names
2. Each record needs to be verified to contain the right amount of data elements to match the columns.
    a. Records that do not match the column count must be written to: <input-filename>-bad.CSV
3. At the end of processing, write statistics to a log file <input-filename>.log
    a. # of records
    b. # of records successful
    c. # of records failed
4. Data sets can be extremely large so be sure the processing is optimized 
5. Application should be re-runnable, meaning that multiple runs with the same input produce the same result
6. It is required that you provide a READE.md thet includes at least 
    a. Summary of the purpose of this repo
    b. Steps for getting this app running
    c. Overview of your approach, design choices, and assumptions

Assumptions:
If the output file or db exist, overwrite them
first line of csv is not entered into the database
columns are always labelled A-J
data does not need to be validated or manipulated other than checking that it exists
only data in column E can be surrounded by ""
all data can be stored as text
row order doesn't matter
output files will be created in the same directory as the input

run instructions:
git clone 
cd to the cloned directory
run:
    mvn compile assembly:single
    java -jar ms3-1.0-jar-with-dependencies.jar {path to input file}
