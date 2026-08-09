---------------------------------------------------
Monitoring and Quality control system for ObsForge.
---------------------------------------------------

The system is centered on a database (sqlite3).
The process is:
1. update inventory
2. inspect inventory
3. generate report

Inventory update can be run at any time: 
the only parameters are the root of the directory tree
to be scanned and the database file to be created or updated
The entire inventory will be scanned, but only new files will 
be added to the database.

Inventory inspection will update the database.
Quality control is done both at the scanning stage and in
the inspection stage.

Reporting consists of a command line interface tool 
to inspect the database and a tool for generating a website.
Website can be generated even if matplotlib is not available,
but then it has no plots.
The website is generated using the database file.
After running on wcoss2 you need to copy the database file to
another machine and generate the website there.

These steps are implemented in the top-level scripts.
The scripts directory contains examples of scripts that
implement this process.

Intended usage:
---------------
A. create a dedicated directory for monitoring
B. copy the run_monitor.sh script to this directory
C. edit this script, providing the path to this directory,
   the root of the data directory,
   the monitoring directory and the name of the database file.
D. if you need to copy the database file to another machine,
   you can create a similar directory on that machine and use
   the generate_website.sh script to create the website from
   the database file.
E. run_monitor.sh can be run as a cron job. It generates a log file
   and keeps adding to it.

