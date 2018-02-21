import pandas as pd
import re
import matplotlib

#Read in the CSV files as dataframes, and create a set to hold them all
data_files = [
    "ap_2010.csv",
    "class_size.csv",
    "demographics.csv",
    "graduation.csv",
    "hs_directory.csv",
    "sat_results.csv"
]
data = {}
for x in data_files:
    data[x.replace('.csv', '')] = pd.read_csv("Raw Data/" + x)

#A list of the fields we want to read in from the two survey files
survey_fields = ["dbn", "rr_s", "rr_t","rr_p", "N_s", "N_t", "N_p", "saf_p_11", "com_p_11", "eng_p_11",
    "aca_p_11", "saf_t_11", "com_t_11", "eng_t_11", "aca_t_11", "saf_s_11", "com_s_11", "eng_s_11",
    "aca_s_11", "saf_tot_11", "com_tot_11", "eng_tot_11", "aca_tot_11"]


#Read in the desired columns from the two surveys as dataframes, append them together, and add them to the set
all_survey = pd.read_csv("Raw Data/survey_all.txt", delimiter="\t", encoding='windows-1252', usecols=survey_fields)
d75_survey = pd.read_csv("Raw Data/survey_d75.txt", delimiter="\t", encoding='windows-1252', usecols=survey_fields)
survey = pd.concat([all_survey, d75_survey], axis=0)
data["survey"] = survey


#Rename the dbn columns so that they match the DBN columns from other datasets
data["survey"].rename(columns={'dbn' : 'DBN'}, inplace=True)
data["hs_directory"].rename(columns={'dbn' : 'DBN'}, inplace=True)


#Add the DBN code for schools in class_size file
data["class_size"]["padded_csd"] = data["class_size"]["CSD"].apply(lambda x: str(x).zfill(2))
data["class_size"]["DBN"] = data["class_size"]["padded_csd"] + data["class_size"]["SCHOOL CODE"]

#Convert SAT subject scores to interger dtype, and create a column representing total SAT score. Then, drop any rows
#with null values
cols = ['SAT Math Avg. Score', 'SAT Critical Reading Avg. Score', 'SAT Writing Avg. Score']
for c in cols:
    data["sat_results"][c] = pd.to_numeric(data["sat_results"][c], errors="coerce")
data['sat_results']['SAT_score'] = data['sat_results'][cols[0]] + data['sat_results'][cols[1]] + data['sat_results'][cols[2]]
data['sat_results'].dropna(inplace=True)

#Some schools are repeated in the AP dataset, so remove the duplicates
data['ap_2010'].drop_duplicates(inplace=True, subset = 'DBN')


#From the Location field, parse out the latitude and longitude, which are written in the format (latitude, longitude)
def find_lat(loc):
    coords = re.findall("\(.+\)", loc)
    lat = coords[0].split(",")[0].replace("(", "")
    return lat
data["hs_directory"]["lat"] = data["hs_directory"]["Location 1"].apply(find_lat)

def find_lon(loc):
    coords = re.findall("\(.+\)", loc)
    lon = coords[0].split(",")[1].replace(")", "").strip()
    return lon
data["hs_directory"]["lon"] = data["hs_directory"]["Location 1"].apply(find_lon)
data["hs_directory"]["lat"] = pd.to_numeric(data["hs_directory"]["lat"], errors="coerce")
data["hs_directory"]["lon"] = pd.to_numeric(data["hs_directory"]["lon"], errors="coerce")


#From the class_size set, remove any rows that don't correspond to general education high schools. Since there are
#still multiple rows for some schools (for example one for english class sizes and one for math sizes), group by the
#dbn, average the class sizes for different subjects
class_size = data["class_size"]
class_size = class_size[(class_size["GRADE "] == "09-12") & (class_size["PROGRAM TYPE"] == "GEN ED")]
class_size = class_size.groupby('DBN').mean()
class_size.reset_index(inplace=True)
data['class_size'] = class_size

#Remove any data that doesn't correspond to the 2011-2012 school year, since that is the year covered by the SAT data
data["demographics"] = data["demographics"][data["demographics"]["schoolyear"] == 20112012]

#Cohort of 2006 is the class of 2010, and is the last cohort for which data is available. Select only data that refers
#to the total cohort, as some rows correspond to only subsections of the school
data["graduation"] = data["graduation"][data["graduation"]["Cohort"] == "2006"]
data["graduation"] = data["graduation"][data["graduation"]["Demographic"] == "Total Cohort"]

#Remove the % character from the data, and convert the data to int type
data['graduation'] = data['graduation'].applymap(lambda x: str(x).replace('%', ''))
data['graduation'] = data['graduation'].applymap(lambda x: str(x).replace('s', ''))
data['graduation'] = data['graduation'].apply(pd.to_numeric, errors='ignore')


#Convert AP data to int type
cols = ['AP Test Takers ', 'Total Exams Taken', 'Number of Exams with scores 3 4 or 5']
for col in cols:
    data["ap_2010"][col] = pd.to_numeric(data["ap_2010"][col], errors="coerce")

#Starting with the sat_results, join the ap and graduation datasets. Left join was used because there are a lot of
#schools for which ap or graduation data is not available
combined = data["sat_results"]
combined = combined.merge(data["ap_2010"], on="DBN", how="left")
combined = combined.merge(data["graduation"], on="DBN", how="left")

#Join the other datasets
to_merge = ["class_size", "demographics", "survey", "hs_directory"]
for m in to_merge:
    combined = combined.merge(data[m], on="DBN", how="inner")

#Fill in missing values with the column mean if the dtype is numeric, else fill them
combined = combined.fillna(combined.mean())
combined = combined.fillna(0)

#From the DBN column, take the first two characters, which correspond to the school district, and add a column to the df
def get_first_two_chars(dbn):
    return dbn[0:2]
combined["school_dist"] = combined["DBN"].apply(get_first_two_chars)

#Write the dataframe to a csv
combined.to_csv('Cleaned data.csv', index=False)