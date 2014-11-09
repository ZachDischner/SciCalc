##! /usr/bin/python
__author__ = 'Zach Dischner'
__copyright__ = "NA"
__credits__ = ["NA"]
__license__ = "NA"
__version__ = "0.0.1"
__maintainer__ = "Zach Dischner"
__email__ = "zach.dischner@gmail.com"
__status__ = "Dev"

"""
File name: SciCalc.py
Authors: Zach Dischner
Created: 7/18/2014
Modified: 7/18/2014


Todos:

"""

####------------------------------------------------------------------------------------------------
####        ---------------------------------------------------------------------------------
####                                        Import and Global 
####        ---------------------------------------------------------------------------------
####------------------------------------------------------------------------------------------------
import sys, os, string, re
import pandas as pd
from datetime import datetime
from ZD_Utils import DataFrameUtils as dfutil
###### Import the Defines.py file, just has clunky variable definitions stuck there
import Defines


###### Set some module-wide variables
_column_keys = Defines.TLCColumns
_keys_to_keep = ["VE__VISIT_DESCRIPTION", "VE__SUMM_VISIT_NAME","VE__SUMM_SAP_DBID", "VE__GNL_DURATION", 
					"VE__SUMM_VISIT_DOYSTART", "VE__VISIT_COMMENTS", "DOY", "DURATION", "DLDV_bits" ]
_num_header_rows = 2
_20DegRate = 1297.95    #bits/sec
_SlewRate = 34.9 		# bits/sec
_TrackDOY = None
_TrackSetupTime = 1 * 60.0 * 60.0 	# 1 hour, in seconds
_TrackSetupTimePerDay = 1 * 60.0 * 60.0 	# 1 hour, in seconds

if float(pd.__version__[2:]) < 14.0:   # pd.__version__ >> '0.14.0'
	raise Exception("NEED PANDAS VERSION 14.0 or higher, upgrade yoself before you wreck yoself.\n\t $pip install pandas>=14.0")

####------------------------------------------------------------------------------------------------
####        ---------------------------------------------------------------------------------
####                                    General helper routines 
####        ---------------------------------------------------------------------------------
####------------------------------------------------------------------------------------------------
def durationFromDescription(description, theType="est"):
	"""Extracts the duration from a visit description string

	Parses a visit description string, and returns 
	the total number of seconds represented therein. The string MUST be in that format. 

	SHOULD PROBABLY BE REWRITTEN TO BE SMARTER, ITS PRETTY DUMB FOR NOW... DEFINITELY ADD REGEX TO CHECK FORMAT BUT IT IS 
	LATE RIGHT NOW AND I'M LAZY

	Args: 
		description: String with description in silly human words

	Kwargs:
		theType: 	{"est" or "exact"} 
						est:	 indicates description is of the form ('Two 8hr Pass. Time TBD')
						exact:	 indicates description is of the form ('Canberra/Madrid DL 05:32 to 23:08')

	Returns:
		duration:	Duration time in seconds

	Examples:
	   	dur = SciCalc.durationFromDescription("Four 8hr Pass...")
	   	>>>: 115200
	"""
	###### Ought to be smarter and use regexes someday maybe...
	# regexify...
	# raise Exception("NOT OF THE CORRECT FORMAT!!! NAMING CONVENTION HAS CHANGED!!! ")
	# or something like that. 

	###### If this description comes from an Estimated track row...
	if string.upper(theType) == "EST":
		desc = description.split(" ")
		## Simple dictionary to convert strings to their number equivalent 
		humanNumbers = {"ONE":1, "TWO":2, "THREE":3, "FOUR":4, "FIVE":5, "SIX":6, "SEVEN":7, "EIGHT":8, "NINE":9, "TEN":10}
		num = humanNumbers[ string.upper(desc[0]) ]
		time = int(re.findall(r'\d+', desc[1])[0])	# Get the '8', should always be 8 but this grabs the numeric value in the string

		###### Assumes the time is always in hours! Can be more robust and check with: time_spec = desc[1][1:], check time_spec...
		duration = num * time * 3600   # number of hours * hours * seconds/hour = seconds

	###### If this description comes from a known track row...
	elif string.upper(theType) == "EXACT":
		## Get the four numbers from the string
		h1,m1,h2,m2=re.findall(r'\d+', description)

		## Calculate number of seconds...
		# For robustness, converts to datetime objects and does a datetime subtraction. 
		duration = (datetime.strptime(":".join([h2,m2]),"%H:%M") - datetime.strptime(":".join([h1,m1]),"%H:%M")).seconds

	else:

		raise Exception("ERROR: Wrong keyword argument passed to SciCalc.durationFromDesctiption(), 'theType (%s) not in ['est','exact']" % theType)


	return duration





####------------------------------------------------------------------------------------------------
####        ---------------------------------------------------------------------------------
####                                    "Visit" Calculation Routines 
####        ---------------------------------------------------------------------------------
####------------------------------------------------------------------------------------------------
def calculateEstimatedTrackVolume(*arg, **kwargs ):
	return 100


def calcluateKnownTrackVolume(*args, **kwargs):
	return 200


def calculateTrackVolume(track_df):
	###### Proper protocol, first make a copy of the dataframe we are screwing with
	df = track_df.copy()

	###### Get the downlink bits available per track
	df["DLDV_bits"] = df.apply(lambda track: _20DegRate * (track["DURATION"]-_TrackSetupTime), axis=1)

	###### Get the index for when a track falls on a new DOY:
	newDOY = df["DOY"]!=df["DOY"].shift(1)

	###### Subtract out the setup/day amount from pertinent tracks
	df["DLDV_bits"][newDOY] -= _TrackSetupTimePerDay * _20DegRate

	###### (IF THINGS GET COMPLICATED, WE CAN DO A MORE TYPICAL FOR LOOP TYPE OPERATION)
	###### Super simple, iterate over rows in the dataframe (one row = one track)
	# for idx,track in df.iterrows():
	# 	track["DLDV_bits"] = _20DegRate * (track["DURATION"]-_TrackSetupTime)
	return df

def getDLDVFromVisit(visit_row):
	"""PROTOTYPE!!!
	This might get much more complicated. Parsing descriptions etc"""
	###### Load visit rate definitions from Defines file
	visitRates = Defines.visitRates
	thisRate = visitRates.get(visit_row["VE__SUMM_VISIT_NAME"])

	return visit_row["DURATION"] * (thisRate or 0)	#thisRate can be None


def calculateScienceVolume(science_df):
	###### Proper protocol, first make a copy of the dataframe we are screwing with
	df = science_df.copy()

	df["DLDV_bits"] = df.apply(lambda obs: getDLDVFromVisit(obs),axis=1)

	return df



####------------------------------------------------------------------------------------------------
####        ---------------------------------------------------------------------------------
####                                        TSV Reading Method 
####        ---------------------------------------------------------------------------------
####------------------------------------------------------------------------------------------------
def importTLC(fname, names=_column_keys):
	"""This function loads a .tlc file into a bunch of pandas dataframes

	The whole file is imported, then subsets are extracted into different dataframes
	*Subset of ful file with only pertinent columns included
	*Known Length Tracks
	*Estimated Length Tracks
	*Everything Else
	Additional information is appended to the returned dataframes including an explicit DOY column and DURATION column

	Args: 
		fname: String with filename and path of excel spreadsheet to open

	Kwargs:
		names:    List of tsv Column names to specify when interpreting. Default comes from Defines.py

	Returns:
		df_full:		Return dataframe representing tlc data
		df:				Full dataframe, with only pertinent columns included
		df_est_track:	Subset of {df} with only playback tracks with unknown/estimated track lengths
		df_exact_track:	Subset of {df} with the exact/known playback tracks

	Examples:
	   	df_full, df, df_est_track, df_exact_track = SciCalc.importTLC(fname)
	"""

	###### Full TSV as a Pandas Dataframe
	df_full = pd.read_table(fname, delimiter="|", skiprows=_num_header_rows, names=_column_keys, index_col=False)
	
	###### Add some columns
	## Split the ...DOYSTART column [2015-025T14:01:00] by "T" then by "-" and grab the DOY part of it...
	df_full["DOY"] = df_full.apply(lambda row: row["VE__SUMM_VISIT_DOYSTART"].split("T")[0].split("-")[1], axis=1)
	## Make it easier... can overwrite later with differently calcualted values, keeps form the same
	df_full["DURATION"] = df_full["VE__GNL_DURATION"].copy()
	## Placeholder
	df_full["DLDV_bits"] = 0

	###### Just keep pertinent columns, because the big thing is clunky
	df = dfutil.colmask(df_full,_keys_to_keep)

	###### Get the subset of the dataframe that corresponds to estimated length tracks
	df_est_track = dfutil.mask(df, "VE__SUMM_VISIT_NAME", "MOPS*EST*", exact=False)
	df_est_track["DURATION"] = df_est_track.apply(lambda row: durationFromDescription(row["VE__VISIT_DESCRIPTION"]), axis=1)
	## Append the function to use to the dataframe??
	# df_est_track["FUNC"] = calculateEstimatedTrackVolume

	###### Get the subset of the dataframe that corresponds to Known length downlink tracks
	df_exact_track = dfutil.mask(df,"VE__SUMM_VISIT_NAME", "*TRK_*", exact=False)

	"""If it is easier, might want to rename columns with df.rename(dict_of_renames{}...)"""

	return df_full, df, df_est_track, df_exact_track


####------------------------------------------------------------------------------------------------
####        ---------------------------------------------------------------------------------
####                                        Main function 
####        ---------------------------------------------------------------------------------
####------------------------------------------------------------------------------------------------
def getDLDV(fname):
	###### Reset the track DOY
	_TrackDOY = None

	###### Parse the file, get dataframes out of it
	df_full, df, df_est_track, df_exact_track = importTLC(fname)

	###### Merge the two track dataframes together... Might want to do this inside of the import function if this is what we use...
	df_Tracks = df_est_track.copy().append(df_exact_track).sort(["VE__SUMM_VISIT_DOYSTART"])

	###### Get "the rest", everything that isn't a track. Again, maybe move this inside of the import funtion...
	df_Science = df.drop(df_Tracks.index)

	###### Make a dictionary of days maybe?
	# doyDict = dict.fromkeys("DOY"+df_full["DOY"].unique())
	df_DOY = pd.DataFrame(columns=["DOY","Available","Downlink","Science"])
	doys = df_full["DOY"].unique()

	####################################################################################
	####### Get the DLDV information
	df_Tracks = calculateTrackVolume(df_Tracks)
	df_Science = calculateScienceVolume(df_Science)

	####################################################################################
	####### Not sure what to do next, get a per-day usage???
	for doy in doys:
		doyKey = "DOY"+doy
		## Add the positive DLDV amount per day from the tracks
		df_DOY.loc[doyKey,"Downlink"] = sum(df_Tracks[df_Tracks["DOY"]==doy]["DLDV_bits"])

		## Subtract the negative DLDV amount from the science activities
		df_DOY.loc[doyKey,"Science"] = sum(df_Science[df_Science["DOY"]==doy]["DLDV_bits"])

		df_DOY.loc[doyKey,"DOY"] = doy

	###### Get the total amount available (Downlink-Science)
	df_DOY["Available"] = df_DOY["Downlink"] - df_DOY["Science"]

	return df_DOY, df_Tracks, df_Science




####################################################################################
####### If module is ran rather than imported
if __name__ == "__main__":
	fname = 'Input/LL_15012_v1inputs_20141013.tsv'

	print "Extracting DLDV info from the TLC file"
	df_DOY, df_T,df_S = SciCalc.getDLDV(fname)



