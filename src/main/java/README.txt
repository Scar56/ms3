Assumptions
If the output file or db exist, overwrite them
first line of csv is not entered into the database
data does not need to be validated or manipulated other than checking that it exists
all data can be stored as text
database and bad,csv files will be created in the same directory as the input

run instructions
from base directory run:
mvn clean compile assembly:single
java -jar ./target/ms3-1.0-jar-with-dependencies.jar {path to input file}
