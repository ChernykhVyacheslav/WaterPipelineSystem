# WaterPipelineSystem

I did not use any frameworks in my application, only jdbc.
I also did not use JavaFX, so results can only be seen in the console.
App reads input.csv, creates a table in h2 database and fills it with data from the file. 
Then it reads routes.csv and then checks whether inquired routes exist and calculates their length, trying to find the shortest path.

Functions that work directly with database are located in DBManager class, while the ones necessary to read and write to files in IOUtils. 
There is Pipeline class that is used to create objects of that type.

In Main class I just use all the functions that do the job. 
First the Pipelines table is created, then data from input file is read and inserted into the database. 
Then method that calculates routes and creates output.csv is called. 
I initially did not use csv directly to create tables, so I created separate function(uploadAllFilesToDB()) in the end that creates three tables for each file and then fills them with corresponding data.