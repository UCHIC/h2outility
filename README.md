# H2O Utility #

A GUI-based and optionally headless tool to select times series values from an ODM 1.1.1 database, write the data to CSV files, and upload them to their respective HydroShare resources. The VisualUpdater can be used to intially select which ODM database, HydroShare resource, and ODM time series should be used.

###### Requirements ######

These tools were originally written for Python 2.7.X 64-bit. 

**NOTE:** Efforts have been made to update the silent updater portion of the tools to Python 3. The requirements listed in the requirements.txt file may be out of date.

To download, install, and run this project, execute the following commands:

> Note: To use a virtual environment with this project, create and activate your virtual environment before running these commands.

```sh
git clone https://github.com/UCHIC/h2outility.git
cd h2outility
python -m pip install -r ./src/requirements.txt
python ./src/VisualUpdater.py
python ./src/SilentUpdater.py
```

***

#### Visual Updater Utility ####

VisualUpdater.py is used to create a series of "rules" used to collect HydroServer time series values, create CSV files, and upload these to HydroShare resources. These rules are stored in a JSON configuration file. To run, simply run the following command (arguments listed below are optional):

```sh
python ./src/VisualUpdater.py
```

| Argument | Description |
| --- | --- |
|`--verbose`|Prints out extra output (lots and lots of extra output)|
|`--debug`|Creates or overwrites log file `Log_File.txt`; stderr output is not redirected to log file|

###### Running the Headless-mode SilentUpdater #####

After you have established the rules for which ODM time series will be written to which HydroShare Resource, you can run the SilentUpdater.py script to update these without needing to use a GUI. This is useful for scheduling run times to keep your HydroShare resources up to date (e.g., as a regular cron job).

To run the Silent Updater, simply run the following command (arguments listed below are optional):
```sh
python ./src/SilentUpdater.py
```

| Argument | Description |
| --- | --- |
|`--verbose`|Prints out extra output (lots and lots of extra output)|
|`--debug`|Creates or overwrites log file `Log_{script}_File.txt`; stderr output is not redirected to log file|

