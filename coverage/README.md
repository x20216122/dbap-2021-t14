There are three scripts in this folder for getting the coverage data.

### pull.py
This connects to the Landsat FTP server and pulls the required data files.

Two types of files are retrieved: 
* the graphical map which identifies the amount of coverage in a 30m square.
* the XML index file that describes the long/lat extents of the map file

### indexsync.py
This script takes the files pulled down by the previous script and pushes
them to the Cosmos (Mongo) database.

The map file name is extracted from the XML file and insert into the mappingfile
table in the postgres database.

### 
