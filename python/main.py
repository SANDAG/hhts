import hhtbs2016Data
import os
import pyodbc
import sqlalchemy
import urllib


# set SQL instance and database containing INRIX objects
server = ""  # TODO: set SQL instance
db = ""  # TODO: set SQL database

# create SQL connection string to database
connStr = "DRIVER={ODBC Driver 17 for SQL Server};" + \
          "SERVER=" + server + ";" + \
          "DATABASE=" + db + ";" + \
          "Trusted_Connection=yes;"

conn = pyodbc.connect(connStr)

engine = sqlalchemy.create_engine(
    "mssql+pyodbc:///?odbc_connect=%s" %
    urllib.parse.quote_plus(connStr))


# day ----
print("Loading: Person Trip Day-Level data")
day = hhtbs2016Data.SurveyData().day

# write person trip day-level data to csv file
dayPath = "../data/sdrts/day.csv"
day.to_csv(dayPath, index=False, sep="|")

# bulk insert person trip day-level data to SQL Server table
with conn.cursor() as cursor:
    sqlBI = "BULK INSERT [hhtbs2016].[day] FROM '" + \
            os.path.realpath(dayPath) + "' " + \
            "WITH (FIRSTROW = 2, TABLOCK, CODEPAGE = 'ACP', " + \
            "FIELDTERMINATOR='|', ROWTERMINATOR='0x0a', MAXERRORS=1);"

    cursor.execute(sqlBI)
    cursor.commit()

os.remove(dayPath)


# households ----
print("Loading: Households data")
hh = hhtbs2016Data.SurveyData().households

# write households data to csv file
hhPath = "../data/sdrts/households.csv"
hh.to_csv(hhPath, index=False, sep="|")

# bulk insert household data to SQL Server temporary table
# then insert to SQL household table transforming WKT to geometry
with conn.cursor() as cursor:
    sqlTT = "DROP TABLE IF EXISTS [hhtbs2016].[tempHH];" \
            "CREATE TABLE [hhtbs2016].[tempHH] (" \
            "[household_id] int NOT NULL," \
            "[sample_segment] nvarchar(50) NOT NULL," \
            "[sample_group] nvarchar(50) NOT NULL," \
            "[travel_date_start] date NULL," \
            "[recruit_survey_where] nvarchar(50) NOT NULL," \
            "[recruit_survey_mobile] nvarchar(50) NOT NULL," \
            "[recruit_survey_start] smalldatetime NOT NULL," \
            "[recruit_survey_end] smalldatetime NOT NULL," \
            "[number_rmove_participants] nvarchar(25) NOT NULL," \
            "[participate_future_studies] nvarchar(10) NOT NULL," \
            "[household_completed] nvarchar(10) NOT NULL," \
            "[completed_days] int NOT NULL," \
            "[language] nvarchar(10) NOT NULL," \
            "[language_other] nvarchar(50) NOT NULL," \
            "[persons] nvarchar(5) NOT NULL," \
            "[adults] nvarchar(5) NOT NULL," \
            "[children] nvarchar(5) NOT NULL," \
            "[workers] nvarchar(5) NOT NULL," \
            "[vehicles] nvarchar(5) NOT NULL," \
            "[bicycles] nvarchar(5) NOT NULL," \
            "[has_share_car] nvarchar(5) NOT NULL," \
            "[has_share_bicycle] nvarchar(5) NOT NULL," \
            "[has_share_vanpool] nvarchar(5) NOT NULL," \
            "[address] nvarchar(150) NOT NULL," \
            "[latitude] float NOT NULL," \
            "[longitude] float NOT NULL," \
            "[shape] nvarchar(max) NOT NULL," \
            "[residence_duration] nvarchar(50) NOT NULL," \
            "[residence_tenure_status] nvarchar(50) NOT NULL," \
            "[residence_type] nvarchar(50) NOT NULL," \
            "[income_category_detailed] nvarchar(25) NOT NULL," \
            "[income_category_broad] nvarchar(25) NOT NULL," \
            "[use_paper_maps] nvarchar(5) NOT NULL," \
            "[freq_paper_maps] nvarchar(25) NOT NULL," \
            "[use_car_navigation] nvarchar(5) NOT NULL," \
            "[freq_car_navigation] nvarchar(25) NOT NULL," \
            "[use_511sd] nvarchar(5) NOT NULL," \
            "[freq_511sd] nvarchar(25) NOT NULL," \
            "[use_apple_maps] nvarchar(5) NOT NULL," \
            "[freq_apple_maps] nvarchar(25) NOT NULL," \
            "[use_car2go] nvarchar(5) NOT NULL," \
            "[freq_car2go] nvarchar(25) NOT NULL," \
            "[use_google_maps] nvarchar(5) NOT NULL," \
            "[freq_google_maps] nvarchar(25) NOT NULL," \
            "[use_icommutesd] nvarchar(5) NOT NULL," \
            "[freq_icommutesd] nvarchar(25) NOT NULL," \
            "[use_lyft] nvarchar(5) NOT NULL," \
            "[freq_lyft] nvarchar(25) NOT NULL," \
            "[use_mapmyride] nvarchar(5) NOT NULL," \
            "[freq_mapmyride] nvarchar(25) NOT NULL," \
            "[use_mapquest] nvarchar(5) NOT NULL," \
            "[freq_mapquest] nvarchar(25) NOT NULL," \
            "[use_sdmts] nvarchar(5) NOT NULL," \
            "[freq_sdmts] nvarchar(25) NOT NULL," \
            "[use_nctd] nvarchar(5) NOT NULL," \
            "[freq_nctd] nvarchar(25) NOT NULL," \
            "[use_waze] nvarchar(5) NOT NULL," \
            "[freq_waze] nvarchar(25) NOT NULL," \
            "[use_uber] nvarchar(5) NOT NULL," \
            "[freq_uber] nvarchar(25) NOT NULL," \
            "[use_other_tool] nvarchar(5) NOT NULL," \
            "[specify_other_tool] nvarchar(150) NOT NULL," \
            "[freq_other_tool] nvarchar(25) NOT NULL," \
            "[use_no_navigation_tools] nvarchar(5) NOT NULL," \
            "[freq_cross_border] nvarchar(25) NOT NULL," \
            "[weight_household_initial] [float] NULL," \
            "[weight_household_4x] [float] NULL," \
            "[weight_household_456x] [float] NULL," \
            "CONSTRAINT [pk_hhtbs2016_tempHH] PRIMARY KEY CLUSTERED ([household_id]))"
    cursor.execute(sqlTT)
    cursor.commit()

    sqlBI = "BULK INSERT [hhtbs2016].[tempHH] FROM '" + \
            os.path.realpath(hhPath) + "' " + \
            "WITH (FIRSTROW = 2, TABLOCK, CODEPAGE = 'ACP', " + \
            "FIELDTERMINATOR='|', ROWTERMINATOR='0x0a', MAXERRORS=1);"

    cursor.execute(sqlBI)
    cursor.commit()

    sqlInsert = "INSERT INTO [hhtbs2016].[households] " \
                "SELECT [household_id], [sample_segment], [sample_group]," \
                "[travel_date_start], [recruit_survey_where]," \
                "[recruit_survey_mobile], [recruit_survey_start]," \
                "[recruit_survey_end], [number_rmove_participants]," \
                "[participate_future_studies], [household_completed]," \
                "[completed_days], [language], [language_other], [persons]," \
                "[adults], [children], [workers], [vehicles], [bicycles]," \
                "[has_share_car], [has_share_bicycle], [has_share_vanpool]," \
                "[address], [latitude], [longitude]," \
                "geometry::STGeomFromText([shape], 2230).MakeValid()," \
                "[residence_duration], [residence_tenure_status]," \
                "[residence_type], [income_category_detailed]," \
                "[income_category_broad], [use_paper_maps]," \
                "[freq_paper_maps], [use_car_navigation]," \
                "[freq_car_navigation], [use_511sd], [freq_511sd]," \
                "[use_apple_maps], [freq_apple_maps], [use_car2go]," \
                "[freq_car2go], [use_google_maps], [freq_google_maps]," \
                "[use_icommutesd], [freq_icommutesd], [use_lyft]," \
                "[freq_lyft], [use_mapmyride], [freq_mapmyride]," \
                "[use_mapquest], [freq_mapquest], [use_sdmts], [freq_sdmts]," \
                "[use_nctd], [freq_nctd], [use_waze], [freq_waze]," \
                "[use_uber], [freq_uber], [use_other_tool]," \
                "[specify_other_tool], [freq_other_tool]," \
                "[use_no_navigation_tools], [freq_cross_border]," \
                "[weight_household_initial], [weight_household_4x]," \
                "[weight_household_456x]" \
                "FROM [hhtbs2016].[tempHH]; " \
                "DROP TABLE [hhtbs2016].[tempHH]"
    cursor.execute(sqlInsert)
    cursor.commit()

os.remove(hhPath)


# trip location points and linestring paths ----
print("Loading: Location data")
locations = hhtbs2016Data.SurveyData().location

# write location data to csv files
linesPath = "../data/sdrts/lines.csv"
pointsPath = "../data/sdrts/points.csv"
locations["lines"].to_csv(linesPath, index=False, sep="|", na_rep="")
locations["points"].to_csv(pointsPath, index=False, sep="|")

# bulk insert location data to SQL Server temporary tables
# then insert to SQL tables transforming WKT to geometry
with conn.cursor() as cursor:
    sqlTT = "DROP TABLE IF EXISTS [hhtbs2016].[tempLines];" \
            "CREATE TABLE [hhtbs2016].[tempLines] (" \
            "[trip_id_location] bigint NOT NULL," \
            "[shape] nvarchar(max) NOT NULL," \
            "CONSTRAINT pk_tempLines PRIMARY KEY CLUSTERED ([trip_id_location]))" \
            "WITH (DATA_COMPRESSION = PAGE);" \
            "DROP TABLE IF EXISTS [hhtbs2016].[tempPoints];" \
            "CREATE TABLE [hhtbs2016].[tempPoints] (" \
            "[point_id] int NOT NULL," \
            "[trip_id_location] bigint NOT NULL," \
            "[collected_at] smalldatetime NOT NULL," \
            "[accuracy] float NULL," \
            "[heading] float NULL," \
            "[speed] float NULL," \
            "[latitude] float NOT NULL," \
            "[longitude] float NOT NULL," \
            "[shape] nvarchar(max) NOT NULL," \
            "CONSTRAINT pk_tempPoints PRIMARY KEY CLUSTERED ([point_id]))" \
            "WITH (DATA_COMPRESSION = PAGE)"
    cursor.execute(sqlTT)
    cursor.commit()

    sqlLinesBI = "BULK INSERT [hhtbs2016].[tempLines] FROM '" + \
                 os.path.realpath(linesPath) + "' " + \
                 "WITH (FIRSTROW = 2, TABLOCK, CODEPAGE = 'ACP', " + \
                 "FIELDTERMINATOR='|', ROWTERMINATOR='0x0a',  MAXERRORS=1);"

    cursor.execute(sqlLinesBI)
    cursor.commit()

    sqlPointsBI = "BULK INSERT [hhtbs2016].[tempPoints] FROM '" + \
                  os.path.realpath(pointsPath) + "' " + \
                  "WITH (FIRSTROW = 2, TABLOCK, CODEPAGE = 'ACP', " + \
                  "FIELDTERMINATOR='|', ROWTERMINATOR='0x0a', MAXERRORS=1);"

    cursor.execute(sqlPointsBI)
    cursor.commit()

    sqlInsert = "INSERT INTO [hhtbs2016].[location_lines] " \
                "SELECT [trip_id_location]," \
                "geometry::STGeomFromText([shape], 2230).MakeValid()" \
                "FROM [hhtbs2016].[tempLines]; " \
                "DROP TABLE [hhtbs2016].[tempLines];" \
                "INSERT INTO [hhtbs2016].[location_points] " \
                "SELECT [point_id], [trip_id_location], [collected_at]," \
                "[accuracy], [heading], [speed], [latitude], [longitude]," \
                "geometry::STGeomFromText([shape], 2230).MakeValid()" \
                "FROM [hhtbs2016].[tempPoints]; " \
                "DROP TABLE [hhtbs2016].[tempPoints];"
    cursor.execute(sqlInsert)
    cursor.commit()

os.remove(linesPath)
os.remove(pointsPath)


# household border trips ----
print("Loading: Household Border Trip data")
borderTrips = hhtbs2016Data.SurveyData().border_trips

# load household border trips pandas DataFrame to SQL
borderTrips.to_sql(name="border_trips",
                   schema="hhtbs2016",
                   con=engine,
                   if_exists="append",
                   index=False)


# intercept ----
print("Loading: AT-Intercept data")
atInt = hhtbs2016Data.SurveyData().intercept

# load AT-intercept pandas DataFrame to SQL
atInt.to_sql(name="intercept",
             schema="hhtbs2016",
             con=engine,
             if_exists="append",
             index=False)


# persons ----
print("Loading: Persons data")
p = hhtbs2016Data.SurveyData().persons

# write persons data to csv file
pPath = "../data/sdrts/persons.csv"
p.to_csv(pPath, index=False, sep="|")

# bulk insert persons data to SQL Server temporary table
# then insert to SQL person table transforming WKT to geometry
with conn.cursor() as cursor:
    sqlTT = "DROP TABLE IF EXISTS [hhtbs2016].[tempPersons];" \
            "CREATE TABLE [hhtbs2016].[tempPersons](" \
            "[person_id] bigint NOT NULL," \
            "[household_id] int NOT NULL," \
            "[person_number] int NOT NULL," \
            "[travel_date_start] date NOT NULL," \
            "[rmove_participant] nvarchar(5) NOT NULL," \
            "[relationship] nvarchar(25) NOT NULL," \
            "[gender] nvarchar(10) NOT NULL," \
            "[age_category] nvarchar(20) NOT NULL," \
            "[employment_status] nvarchar(50) NOT NULL," \
            "[number_of_jobs] nvarchar(15) NOT NULL," \
            "[adult_student_status] nvarchar(20) NOT NULL," \
            "[educational_attainment] nvarchar(30) NOT NULL," \
            "[drivers_license] nvarchar(15) NOT NULL," \
            "[military_status] nvarchar(65) NOT NULL," \
            "[ethnicity_americanindian_alaskanative] nvarchar(15) NOT NULL," \
            "[ethnicity_asian] nvarchar(15) NOT NULL," \
            "[ethnicity_black] nvarchar(15) NOT NULL," \
            "[ethnicity_hispanic] nvarchar(15) NOT NULL," \
            "[ethnicity_hawaiian_pacific] nvarchar(15) NOT NULL," \
            "[ethnicity_white] nvarchar(15) NOT NULL," \
            "[ethnicity_other] nvarchar(15) NOT NULL," \
            "[disability] nvarchar(25) NOT NULL," \
            "[height] float NULL," \
            "[weight] float NULL," \
            "[physical_activity] nvarchar(50) NOT NULL," \
            "[transit_frequency] nvarchar(20) NOT NULL," \
            "[transit_pass] nvarchar(65) NOT NULL," \
            "[school_type] nvarchar(45) NOT NULL," \
            "[school_frequency] nvarchar(35) NOT NULL," \
            "[other_school] nvarchar(30) NOT NULL," \
            "[school_mode] nvarchar(55) NOT NULL," \
            "[daycare_open] nvarchar(15) NOT NULL," \
            "[daycare_close] nvarchar(15) NOT NULL," \
            "[work_location_type] nvarchar(75) NOT NULL," \
            "[occupation] nvarchar(45) NOT NULL," \
            "[industry] nvarchar(55) NOT NULL," \
            "[hours_worked] nvarchar(40) NOT NULL," \
            "[commute_frequency] nvarchar(25) NOT NULL," \
            "[commute_mode] nvarchar(55) NOT NULL," \
            "[work_arrival_frequency] nvarchar(50) NOT NULL," \
            "[work_parking_payment] nvarchar(75) NOT NULL," \
            "[work_parking_cost] float NULL," \
            "[work_parking_cost_dk] nvarchar(15) NOT NULL," \
            "[work_parking_ease] nvarchar(65) NOT NULL," \
            "[telecommute_frequency] nvarchar(25) NOT NULL," \
            "[commute_subsidy_none] nvarchar(15) NOT NULL," \
            "[commute_subsidy_parking] nvarchar(15) NOT NULL," \
            "[commute_subsidy_transit] nvarchar(15) NOT NULL," \
            "[commute_subsidy_vanpool] nvarchar(15) NOT NULL," \
            "[commute_subsidy_cash] nvarchar(15) NOT NULL," \
            "[commute_subsidy_other] nvarchar(15) NOT NULL," \
            "[commute_subsidy_specify] nvarchar(150) NOT NULL," \
            "[has_second_home] nvarchar(5) NOT NULL," \
            "[second_home_address] nvarchar(110) NOT NULL," \
            "[second_home_latitude] float NULL," \
            "[second_home_longitude] float NULL," \
            "[second_home_shape] nvarchar(max) NULL," \
            "[school_address] nvarchar(110) NOT NULL," \
            "[school_latitude] float NULL," \
            "[school_longitude] float NULL," \
            "[school_shape] nvarchar(max) NULL," \
            "[second_school_address] nvarchar(110) NOT NULL," \
            "[second_school_latitude] float NULL," \
            "[second_school_longitude] float NULL," \
            "[second_school_shape] nvarchar(max) NULL," \
            "[work_address] nvarchar(110) NOT NULL," \
            "[work_latitude] float NULL," \
            "[work_longitude] float NULL," \
            "[work_shape] nvarchar(max) NULL," \
            "[second_work_address] nvarchar(110) NOT NULL," \
            "[second_work_latitude] float NULL," \
            "[second_work_longitude] float NULL," \
            "[second_work_shape] nvarchar(max) NULL," \
            "[smartphone_type] nvarchar(35) NOT NULL," \
            "[smartphone_age] nvarchar(15) NOT NULL," \
            "[smartphone_child] nvarchar(15) NOT NULL," \
            "[diary_callcenter] nvarchar(15) NOT NULL," \
            "[diary_mobile] nvarchar(15) NOT NULL," \
            "[rmove_activated] smalldatetime NULL," \
            "[completed_days] float NULL," \
            "[completed_day1] nvarchar(15) NOT NULL," \
            "[completed_day2] nvarchar(15) NOT NULL," \
            "[completed_day3] nvarchar(15) NOT NULL," \
            "[completed_day4] nvarchar(15) NOT NULL," \
            "[completed_day5] nvarchar(15) NOT NULL," \
            "[completed_day6] nvarchar(15) NOT NULL," \
            "[completed_day7] nvarchar(15) NOT NULL," \
            "CONSTRAINT [pk_hhtbs2016_tempPersons] PRIMARY KEY CLUSTERED ([person_id]))"
    cursor.execute(sqlTT)
    cursor.commit()

    sqlBI = "BULK INSERT [hhtbs2016].[tempPersons] FROM '" + \
            os.path.realpath(pPath) + "' " + \
            "WITH (FIRSTROW = 2, TABLOCK, CODEPAGE = 'ACP', " + \
            "FIELDTERMINATOR='|', ROWTERMINATOR='0x0a', MAXERRORS=1);"

    cursor.execute(sqlBI)
    cursor.commit()

    sqlInsert = "INSERT INTO [hhtbs2016].[persons] " \
                "SELECT [person_id], [household_id], [person_number]," \
                "[travel_date_start], [rmove_participant], [relationship]," \
                "[gender], [age_category], [employment_status], [number_of_jobs]," \
                "[adult_student_status], [educational_attainment]," \
                "[drivers_license], [military_status]," \
                "[ethnicity_americanindian_alaskanative], [ethnicity_asian]," \
                "[ethnicity_black], [ethnicity_hispanic]," \
                "[ethnicity_hawaiian_pacific], [ethnicity_white]," \
                "[ethnicity_other], [disability], [height], [weight]," \
                "[physical_activity], [transit_frequency], [transit_pass]," \
                "[school_type], [school_frequency], [other_school], [school_mode]," \
                "[daycare_open], [daycare_close], [work_location_type]," \
                "[occupation], [industry], [hours_worked], [commute_frequency]," \
                "[commute_mode], [work_arrival_frequency], [work_parking_payment]," \
                "[work_parking_cost], [work_parking_cost_dk], [work_parking_ease]," \
                "[telecommute_frequency], [commute_subsidy_none]," \
                "[commute_subsidy_parking], [commute_subsidy_transit]," \
                "[commute_subsidy_vanpool], [commute_subsidy_cash]," \
                "[commute_subsidy_other], [commute_subsidy_specify]," \
                "[has_second_home], [second_home_address]," \
                "[second_home_latitude], [second_home_longitude]," \
                "geometry::STGeomFromText([second_home_shape], 2230).MakeValid()," \
                "[school_address], [school_latitude], [school_longitude]," \
                "geometry::STGeomFromText([school_shape], 2230).MakeValid()," \
                "[second_school_address], [second_school_latitude]," \
                "[second_school_longitude]," \
                "geometry::STGeomFromText([second_school_shape], 2230).MakeValid()," \
                "[work_address], [work_latitude], [work_longitude]," \
                "geometry::STGeomFromText([work_shape], 2230).MakeValid()," \
                "[second_work_address], [second_work_latitude], [second_work_longitude]," \
                "geometry::STGeomFromText([second_work_shape], 2230).MakeValid()," \
                "[smartphone_type], [smartphone_age], [smartphone_child]," \
                "[diary_callcenter], [diary_mobile], [rmove_activated]," \
                "[completed_days], [completed_day1], [completed_day2]," \
                "[completed_day3], [completed_day4], [completed_day5]," \
                "[completed_day6], [completed_day7]" \
                "FROM [hhtbs2016].[tempPersons]; " \
                "DROP TABLE [hhtbs2016].[tempPersons]"
    cursor.execute(sqlInsert)
    cursor.commit()

os.remove(pPath)


# trip list ----
print("Loading: Trip List data")
trips = hhtbs2016Data.SurveyData().trips

# write trip data to csv file
tripsPath = "../data/sdrts/trips.csv"
trips.to_csv(tripsPath, index=False, sep="|")

# bulk insert trip data to SQL Server temporary table
# then insert to SQL trip table transforming WKT to geometry
with conn.cursor() as cursor:
    sqlTT = "DROP TABLE IF EXISTS [hhtbs2016].[tempTrips];" \
            "CREATE TABLE [hhtbs2016].[tempTrips](" \
            "[trip_id] bigint NOT NULL," \
            "[trip_id_linked] bigint NOT NULL," \
            "[trip_id_location] float NOT NULL," \
            "[person_id] bigint NOT NULL," \
            "[household_id] int NOT NULL," \
            "[travel_date] date NOT NULL," \
            "[travel_day_number] float NULL," \
            "[travel_day_of_week] nvarchar(10) NOT NULL," \
            "[data_source] nvarchar(10) NOT NULL," \
            "[completed_trip_survey] nvarchar(5) NOT NULL," \
            "[completed_date] smalldatetime NULL," \
            "[completed_household_survey] nvarchar(15) NOT NULL," \
            "[completed_person_survey] nvarchar(15) NOT NULL," \
            "[number_household_survey_weekdays] float NULL," \
            "[revised_at] smalldatetime NULL," \
            "[revised_count] nvarchar(15) NOT NULL," \
            "[error] nvarchar(20) NOT NULL," \
            "[flag_teleport] nvarchar(15) NOT NULL," \
            "[copied_trip] nvarchar(15) NOT NULL," \
            "[analyst_merged_trip] nvarchar(15) NOT NULL," \
            "[analyst_split_trip] nvarchar(15) NOT NULL," \
            "[user_merged_trip] nvarchar(15) NOT NULL," \
            "[user_split_trip] nvarchar(15) NOT NULL," \
            "[added_trip] nvarchar(15) NOT NULL," \
            "[nonproxy_derived_trip] nvarchar(15) NOT NULL," \
            "[proxy_added_trip] nvarchar(15) NOT NULL," \
            "[unlinked_transit_trip] nvarchar(5) NOT NULL," \
            "[origin_name] nvarchar(150) NOT NULL," \
            "[origin_address] nvarchar(150) NOT NULL," \
            "[origin_latitude] float NULL," \
            "[origin_longitude] float NULL," \
            "[origin_shape] nvarchar(MAX) NULL," \
            "[destination_name] nvarchar(150) NOT NULL," \
            "[destination_address] nvarchar(150) NOT NULL," \
            "[destination_latitude] float NULL," \
            "[destination_longitude] float NULL," \
            "[destination_shape] nvarchar(MAX) NULL," \
            "[origin_purpose] nvarchar(60) NOT NULL," \
            "[origin_purpose_other_specify] nvarchar(150) NOT NULL," \
            "[origin_purpose_inferred] nvarchar(50) NOT NULL," \
            "[destination_purpose] nvarchar(60) NOT NULL," \
            "[destination_purpose_other_specify] nvarchar(150) NOT NULL," \
            "[destination_purpose_inferred] nvarchar(50) NOT NULL," \
            "[departure_time] smalldatetime NOT NULL," \
            "[arrival_time] smalldatetime NOT NULL," \
            "[travelers] nvarchar(25) NOT NULL," \
            "[travelers_household] nvarchar(25) NOT NULL," \
            "[travelers_non_household] nvarchar(25) NOT NULL," \
            "[mode_1] nvarchar(30) NOT NULL," \
            "[mode_2] nvarchar(30) NOT NULL," \
            "[mode_3] nvarchar(30) NOT NULL," \
            "[mode_4] nvarchar(30) NOT NULL," \
            "[mode_transit_access] nvarchar(35) NOT NULL," \
            "[mode_transit_egress] nvarchar(35) NOT NULL," \
            "[google_mode] nvarchar(15) NOT NULL," \
            "[driver] nvarchar(40) NOT NULL," \
            "[toll_road] nvarchar(15) NOT NULL," \
            "[toll_road_express] nvarchar(15) NOT NULL," \
            "[parking_location] nvarchar(50) NOT NULL," \
            "[parking_pay_type] nvarchar(50) NOT NULL," \
            "[parking_cost] float NULL," \
            "[parking_cost_dk] nvarchar(15) NULL," \
            "[parking_egress_duration] float NULL," \
            "[taxi_pay_type] nvarchar(45) NULL," \
            "[taxi_cost] float NULL," \
            "[taxi_cost_dk] nvarchar(15) NULL," \
            "[airplane_pay_type] nvarchar(45) NOT NULL," \
            "[airplane_cost] float NULL," \
            "[airplane_cost_dk] nvarchar(15) NULL," \
            "[bus_pay_type] nvarchar(35) NULL," \
            "[bus_cost] float NULL," \
            "[bus_cost_dk] nvarchar(15) NULL," \
            "[rail_pay_type] nvarchar(35) NULL," \
            "[rail_cost] float NULL," \
            "[rail_cost_dk] nvarchar(15) NULL," \
            "[ferry_pay_type] nvarchar(35) NULL," \
            "[ferry_cost] float NULL," \
            "[ferry_cost_dk] nvarchar(15) NULL," \
            "[parkride_lot] nvarchar(40) NULL," \
            "[parkride_city] nvarchar(15) NULL," \
            "[distance] float NULL," \
            "[duration] float NULL," \
            "[duration_reported] float NULL," \
            "[speed] float NULL," \
            "[weight_trip] float NULL," \
            "[weight_person_trip] float NULL," \
            "[weight_household_multiday_factor] float NULL," \
            "[weight_person_multiday_456x] float NULL," \
            "CONSTRAINT [pk_hhtbs2016_tempTrips] PRIMARY KEY CLUSTERED ([trip_id]))"
    cursor.execute(sqlTT)
    cursor.commit()

    sqlBI = "BULK INSERT [hhtbs2016].[tempTrips] FROM '" + \
            os.path.realpath(tripsPath) + "' " + \
            "WITH (FIRSTROW = 2, TABLOCK, CODEPAGE = 'ACP', " + \
            "FIELDTERMINATOR='|', ROWTERMINATOR='0x0a', MAXERRORS=1);"

    cursor.execute(sqlBI)
    cursor.commit()

    sqlInsert = "INSERT INTO [hhtbs2016].[trips] " \
                "SELECT [trip_id], [trip_id_linked], [trip_id_location]," \
                "[person_id], [household_id], [travel_date], [travel_day_number]," \
                "[travel_day_of_week], [data_source], [completed_trip_survey]," \
                "[completed_date], [completed_household_survey]," \
                "[completed_person_survey], [number_household_survey_weekdays]," \
                "[revised_at], [revised_count], [error], [flag_teleport]," \
                "[copied_trip], [analyst_merged_trip], [analyst_split_trip]," \
                "[user_merged_trip], [user_split_trip], [added_trip]," \
                "[nonproxy_derived_trip], [proxy_added_trip]," \
                "[unlinked_transit_trip], [origin_name], [origin_address]," \
                "[origin_latitude], [origin_longitude]," \
                "geometry::STGeomFromText([origin_shape], 2230).MakeValid()," \
                "[destination_name], [destination_address]," \
                "[destination_latitude], [destination_longitude]," \
                "geometry::STGeomFromText([destination_shape], 2230).MakeValid()," \
                "[origin_purpose], [origin_purpose_other_specify]," \
                "[origin_purpose_inferred], [destination_purpose]," \
                "[destination_purpose_other_specify], [destination_purpose_inferred]," \
                "[departure_time], [arrival_time], [travelers]," \
                "[travelers_household], [travelers_non_household], [mode_1]," \
                "[mode_2], [mode_3], [mode_4], [mode_transit_access]," \
                "[mode_transit_egress], [google_mode], [driver], [toll_road]," \
                "[toll_road_express], [parking_location], [parking_pay_type]," \
                "[parking_cost], [parking_cost_dk], [parking_egress_duration]," \
                "[taxi_pay_type], [taxi_cost], [taxi_cost_dk], [airplane_pay_type]," \
                "[airplane_cost], [airplane_cost_dk], [bus_pay_type], [bus_cost]," \
                "[bus_cost_dk], [rail_pay_type], [rail_cost], [rail_cost_dk]," \
                "[ferry_pay_type], [ferry_cost], [ferry_cost_dk], [parkride_lot]," \
                "[parkride_city], [distance], [duration], [duration_reported]," \
                "[speed], [weight_trip], [weight_person_trip]," \
                "[weight_household_multiday_factor], [weight_person_multiday_456x]" \
                "FROM [hhtbs2016].[tempTrips]; " \
                "DROP TABLE [hhtbs2016].[tempTrips]"
    cursor.execute(sqlInsert)
    cursor.commit()

os.remove(tripsPath)


# vehicles ----
print("Loading: Vehicle data")
vehicles = hhtbs2016Data.SurveyData().vehicles

# load vehicles pandas DataFrame to SQL
vehicles.to_sql(name="vehicles",
                schema="hhtbs2016",
                con=engine,
                if_exists="append",
                index=False)
