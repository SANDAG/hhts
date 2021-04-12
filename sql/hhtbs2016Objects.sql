SET NOCOUNT ON
GO


/*****************************************************************************/
-- create HHTBS 2016 schema and tables

-- schemas
CREATE SCHEMA [hhtbs2016]
GO


-- day
CREATE TABLE [hhtbs2016].[day] (
    [day_id] int NOT NULL,
    [person_id] bigint NOT NULL,
	[household_id] int NOT NULL,
	[travel_date] date NOT NULL,
	[travel_day_number] tinyint NOT NULL,
	[travel_day_of_week] nvarchar(10) NOT NULL,
	[data_source] nvarchar(10) NOT NULL,
	[completed_household_survey] nvarchar(20) NOT NULL,
	[completed_person_survey] nvarchar(5) NOT NULL,
	[completed_date] smalldatetime NULL,
	[revised_at] smalldatetime NULL,
	[revised_count] int NULL,
	[diary_start_time] smalldatetime NULL,
	[diary_end_time] smalldatetime NULL,
	[diary_duration] int NULL,
	[survey_status] nvarchar(40) NOT NULL,
	[proxy] nvarchar(50) NOT NULL,
	[made_trips] nvarchar(5) NOT NULL,
	[no_trips_reason_1] nvarchar(50) NOT NULL,
	[no_trips_reason_2] nvarchar(50) NOT NULL,
	[no_trips_reason_specify_other] nvarchar(100) NOT NULL,
	[number_trips] int NOT NULL,
	[number_surveys] int NOT NULL,
	[start_location] nvarchar(20) NOT NULL,
	[start_location_other] nvarchar(150) NOT NULL,
	[end_location] nvarchar(20) NOT NULL,
	[end_location_other] nvarchar(150) NOT NULL,
	[time_telework] float NULL,
	[time_shop] float NULL,
	[toll_road] nvarchar(25) NOT NULL,
	[toll_road_express] nvarchar(25) NOT NULL,
	[deliver_package] nvarchar(25) NOT NULL,
	[deliver_food] nvarchar(25) NOT NULL,
	[deliver_work] nvarchar(25) NOT NULL,
	[weight_household_456x] float NULL,
	[weight_household_multiday_factor] float NULL,
	[weight_person_multiday_456x] float NULL,
    CONSTRAINT pk_day PRIMARY KEY CLUSTERED ([day_id]),
    CONSTRAINT ixuq_day UNIQUE ([person_id], [travel_date]))
WITH (DATA_COMPRESSION = PAGE)
GO


-- households
CREATE TABLE [hhtbs2016].[households] (
    [household_id] integer NOT NULL,
    [sample_segment] nvarchar(50) NOT NULL,
    [sample_group] nvarchar(50) NOT NULL,
    [travel_date_start] date NULL,
    [recruit_survey_where] nvarchar(50) NOT NULL,
    [recruit_survey_mobile] nvarchar(50) NOT NULL,
    [recruit_survey_start] smalldatetime NOT NULL,
    [recruit_survey_end] smalldatetime NOT NULL,
    [number_rmove_participants] nvarchar(25) NOT NULL,
    [participate_future_studies] nvarchar(10) NOT NULL,
    [household_completed] nvarchar(10) NOT NULL,
    [completed_days] integer NOT NULL,
    [language] nvarchar(10) NOT NULL,
    [language_other] nvarchar(50) NOT NULL,
    [persons] nvarchar(5) NOT NULL,
    [adults] nvarchar(5) NOT NULL,
    [children] nvarchar(5) NOT NULL,
    [workers] nvarchar(5) NOT NULL,
    [vehicles] nvarchar(5) NOT NULL,
    [bicycles] nvarchar(5) NOT NULL,
    [has_share_car] nvarchar(5) NOT NULL,
    [has_share_bicycle] nvarchar(5) NOT NULL,
    [has_share_vanpool] nvarchar(5) NOT NULL,
    [address] nvarchar(150) NOT NULL,
    [latitude] float NOT NULL,
    [longitude] float NOT NULL,
    [shape] geometry NOT NULL,
    [mgra_13] integer NOT NULL,
    [residence_duration] nvarchar(50) NOT NULL,
    [residence_tenure_status] nvarchar(50) NOT NULL,
    [residence_type] nvarchar(50) NOT NULL,
    [income_category_detailed] nvarchar(25) NOT NULL,
    [income_category_broad] nvarchar(25) NOT NULL,
    [use_paper_maps] nvarchar(5) NOT NULL,
    [freq_paper_maps] nvarchar(25) NOT NULL,
    [use_car_navigation] nvarchar(5) NOT NULL,
    [freq_car_navigation] nvarchar(25) NOT NULL,
    [use_511sd] nvarchar(5) NOT NULL,
    [freq_511sd] nvarchar(25) NOT NULL,
    [use_apple_maps] nvarchar(5) NOT NULL,
    [freq_apple_maps] nvarchar(25) NOT NULL,
    [use_car2go] nvarchar(5) NOT NULL,
    [freq_car2go] nvarchar(25) NOT NULL,
    [use_google_maps] nvarchar(5) NOT NULL,
    [freq_google_maps] nvarchar(25) NOT NULL,
    [use_icommutesd] nvarchar(5) NOT NULL,
    [freq_icommutesd] nvarchar(25) NOT NULL,
    [use_lyft] nvarchar(5) NOT NULL,
    [freq_lyft] nvarchar(25) NOT NULL,
    [use_mapmyride] nvarchar(5) NOT NULL,
    [freq_mapmyride] nvarchar(25) NOT NULL,
    [use_mapquest] nvarchar(5) NOT NULL,
    [freq_mapquest] nvarchar(25) NOT NULL,
    [use_sdmts] nvarchar(5) NOT NULL,
    [freq_sdmts] nvarchar(25) NOT NULL,
    [use_nctd] nvarchar(5) NOT NULL,
    [freq_nctd] nvarchar(25) NOT NULL,
    [use_waze] nvarchar(5) NOT NULL,
    [freq_waze] nvarchar(25) NOT NULL,
    [use_uber] nvarchar(5) NOT NULL,
    [freq_uber] nvarchar(25) NOT NULL,
    [use_other_tool] nvarchar(5) NOT NULL,
    [specify_other_tool] nvarchar(150) NOT NULL,
    [freq_other_tool] nvarchar(25) NOT NULL,
    [use_no_navigation_tools] nvarchar(5) NOT NULL,
    [freq_cross_border] nvarchar(25) NOT NULL,
    [weight_household_initial] float NULL,
    [weight_household_4x] float NULL,
    [weight_household_456x] float NULL,
    CONSTRAINT [pk_hhtbs2016_households] PRIMARY KEY ([household_id]))
WITH (DATA_COMPRESSION = PAGE)
GO


-- household border trip list
CREATE TABLE [hhtbs2016].[border_trips] (
	[border_trip_id] integer NOT NULL,
	[household_id] integer NOT NULL,
	[trip_id] integer NOT NULL,
    [mode] nvarchar(75) NOT NULL,
    [port_of_entry] nvarchar(75) NOT NULL,
    [purpose] nvarchar(75) NOT NULL,
    [duration] nvarchar(25) NOT NULL,
    [party_size] nvarchar(50) NOT NULL,
    CONSTRAINT [pk_hhtbs2016_border_trips] PRIMARY KEY ([border_trip_id]),
    CONSTRAINT [ixuq_hhtbs2016_border_trips] UNIQUE ([household_id], [trip_id])
    )
WITH (DATA_COMPRESSION = PAGE)
GO


-- AT-intercept survey
CREATE TABLE [hhtbs2016].[intercept] (
	[household_id] int NOT NULL,
	[survey_status] nvarchar(10) NOT NULL,
	[survey_start] smalldatetime NOT NULL,
	[survey_end] smalldatetime NOT NULL,
	[survey_date] date NOT NULL,
	[pilot_study] nvarchar(10) NOT NULL,
	[origin_purpose] nvarchar(65) NOT NULL,
	[employment_status] nvarchar(50) NOT NULL,
	[student_status] nvarchar(60) NOT NULL,
	[origin_address] nvarchar(125) NOT NULL,
	[origin_latitude] float NOT NULL,
	[origin_longitude] float NOT NULL,
	[origin_shape] geometry NOT NULL,
    [origin_mgra_13] integer NULL,  -- allow NULLs
	[destination_purpose] nvarchar(65) NOT NULL,
	[destination_address] nvarchar(125) NOT NULL,
	[destination_latitude] float NOT NULL,
	[destination_longitude] float NOT NULL,
	[destination_shape] geometry NOT NULL,
    [destination_mgra_13] integer NULL,  -- allow NULLs
	[distance_beeline] float NOT NULL,
	[distance_beeline_bin] nvarchar(20) NOT NULL,
	[visit_work] nvarchar(20) NOT NULL,
	[visit_school] nvarchar(20) NOT NULL,
	[number_household_vehicles] nvarchar(10) NOT NULL,
	[number_children_0_15] tinyint NOT NULL,
	[number_children_16_17] tinyint NOT NULL,
	[number_adults] tinyint NOT NULL,
	[age] nvarchar(20) NOT NULL,
	[smartphone] nvarchar(35) NOT NULL,
	[resident] nvarchar(10) NOT NULL,
	[bike_party] nvarchar(10) NOT NULL,
	[bike_share] nvarchar(10) NOT NULL,
	[gender] nvarchar(10) NOT NULL,
	[intercept_site] nvarchar(85) NOT NULL,
	[intercept_direction] nvarchar(10) NOT NULL,
	[language] nvarchar(10) NOT NULL,
	[rmove_qualify] nvarchar(10) NOT NULL,
	[opt_out] nvarchar(15) NOT NULL,
	[rmove_participate] nvarchar(10) NOT NULL,
	[rmove_complete] nvarchar(15) NOT NULL,
	[recruit_complete] nvarchar(20) NOT NULL,
	[survey_time_peak] nvarchar(10) NOT NULL,
	[expansion_site] nvarchar(85) NOT NULL,
	[expansion_factor] float NULL,
    CONSTRAINT [pk_hhtbs2016_intercept] PRIMARY KEY ([household_id])
    )
WITH (DATA_COMPRESSION = PAGE)
GO


-- trip location lines
CREATE TABLE [hhtbs2016].[location_lines] (
    [trip_id_location] bigint NOT NULL,
    [shape] geometry NOT NULL,
    CONSTRAINT pk_location_lines PRIMARY KEY CLUSTERED ([trip_id_location]))
WITH (DATA_COMPRESSION = PAGE)
GO


-- trip location points
CREATE TABLE [hhtbs2016].[location_points] (
    [point_id] int NOT NULL,
    [trip_id_location] bigint NOT NULL,
    [collected_at] smalldatetime NOT NULL,
    [accuracy] float NULL,
    [heading] float NULL,
    [speed] float NULL,
    [latitude] float NOT NULL,
    [longitude] float NOT NULL,
    [shape] geometry NOT NULL,
    CONSTRAINT pk_location_points PRIMARY KEY CLUSTERED ([point_id]))
WITH (DATA_COMPRESSION = PAGE)
GO


-- persons
CREATE TABLE [hhtbs2016].[persons] (
    [person_id] bigint NOT NULL,
    [household_id] integer NOT NULL,
    [person_number] integer NOT NULL,
    [travel_date_start] date NOT NULL,
    [rmove_participant] nvarchar(5) NOT NULL,
    [relationship] nvarchar(25) NOT NULL,
    [gender] nvarchar(10) NOT NULL,
    [age_category] nvarchar(20) NOT NULL,
    [employment_status] nvarchar(50) NOT NULL,
    [number_of_jobs] nvarchar(15) NOT NULL,
    [adult_student_status] nvarchar(20) NOT NULL,
    [educational_attainment] nvarchar(30) NOT NULL,
    [drivers_license] nvarchar(15) NOT NULL,
    [military_status] nvarchar(65) NOT NULL,
    [ethnicity_americanindian_alaskanative] nvarchar(15) NOT NULL,
    [ethnicity_asian] nvarchar(15) NOT NULL,
    [ethnicity_black] nvarchar(15) NOT NULL,
    [ethnicity_hispanic] nvarchar(15) NOT NULL,
    [ethnicity_hawaiian_pacific] nvarchar(15) NOT NULL,
    [ethnicity_white] nvarchar(15) NOT NULL,
    [ethnicity_other] nvarchar(15) NOT NULL,
    [disability] nvarchar(25) NOT NULL,
    [height] integer NULL,  -- allow NULLs
    [weight] integer NULL,  -- allow NULLs
    [physical_activity] nvarchar(50) NOT NULL,
    [transit_frequency] nvarchar(20) NOT NULL,
    [transit_pass] nvarchar(65) NOT NULL,
    [school_type] nvarchar(45) NOT NULL,
    [school_frequency] nvarchar(35) NOT NULL,
    [other_school] nvarchar(30) NOT NULL,
    [school_mode] nvarchar(55) NOT NULL,
    [daycare_open] nvarchar(15) NOT NULL,
    [daycare_close] nvarchar(15) NOT NULL,
    [work_location_type] nvarchar(75) NOT NULL,
    [occupation] nvarchar(45) NOT NULL,
    [industry] nvarchar(55) NOT NULL,
    [hours_worked] nvarchar(40) NOT NULL,
    [commute_frequency] nvarchar(25) NOT NULL,
    [commute_mode] nvarchar(55) NOT NULL,
    [work_arrival_frequency] nvarchar(50) NOT NULL,
    [work_parking_payment] nvarchar(75) NOT NULL,
    [work_parking_cost] integer NULL,  -- allow NULLs
    [work_parking_cost_dk] nvarchar(15) NOT NULL,
    [work_parking_ease] nvarchar(65) NOT NULL,
    [telecommute_frequency] nvarchar(25) NOT NULL,
    [commute_subsidy_none] nvarchar(15) NOT NULL,
    [commute_subsidy_parking] nvarchar(15) NOT NULL,
    [commute_subsidy_transit] nvarchar(15) NOT NULL,
    [commute_subsidy_vanpool] nvarchar(15) NOT NULL,
    [commute_subsidy_cash] nvarchar(15) NOT NULL,
    [commute_subsidy_other] nvarchar(15) NOT NULL,
    [commute_subsidy_specify] nvarchar(150) NOT NULL,
    [has_second_home] nvarchar(5) NOT NULL,
    [second_home_address] nvarchar(110) NOT NULL,
    [second_home_latitude] float NULL,  -- allow NULLS
    [second_home_longitude] float NULL,  -- allow NULLS
    [second_home_shape] geometry NULL,  -- allow NULLS
    [second_home_mgra_13] integer NULL,  -- allow NULLS
    [school_address] nvarchar(110) NOT NULL,
    [school_latitude] float NULL,  -- allow NULLS
    [school_longitude] float NULL,  -- allow NULLS
    [school_shape] geometry NULL,  -- allow NULLS
    [school_mgra_13] integer NULL,  -- allow NULLS
    [second_school_address] nvarchar(110) NOT NULL,
    [second_school_latitude] float NULL,  -- allow NULLS
    [second_school_longitude] float NULL,  -- allow NULLS
    [second_school_shape] geometry NULL,  -- allow NULLS
    [second_school_mgra_13] integer NULL,  -- allow NULLS
    [work_address] nvarchar(110) NOT NULL,
    [work_latitude] float NULL,  -- allow NULLS
    [work_longitude] float NULL,  -- allow NULLS
    [work_shape] geometry NULL,  -- allow NULLS
    [work_mgra_13] integer NULL,  -- allow NULLS
    [second_work_address] nvarchar(110) NOT NULL,
    [second_work_latitude] float NULL,  -- allow NULLS
    [second_work_longitude] float NULL,  -- allow NULLS
    [second_work_shape] geometry NULL,  -- allow NULLS
    [second_work_mgra_13] integer NULL,  -- allow NULLS
    [smartphone_type] nvarchar(35) NOT NULL,
    [smartphone_age] nvarchar(15) NOT NULL,
    [smartphone_child] nvarchar(15) NOT NULL,
    [diary_callcenter] nvarchar(15) NOT NULL,
    [diary_mobile] nvarchar(15) NOT NULL,
    [rmove_activated] smalldatetime NULL,  -- allow NULLs
    [completed_days] integer NULL,
    [completed_day1] nvarchar(15) NOT NULL,
    [completed_day2] nvarchar(15) NOT NULL,
    [completed_day3] nvarchar(15) NOT NULL,
    [completed_day4] nvarchar(15) NOT NULL,
    [completed_day5] nvarchar(15) NOT NULL,
    [completed_day6] nvarchar(15) NOT NULL,
    [completed_day7] nvarchar(15) NOT NULL,
    CONSTRAINT [pk_hhtbs2016_persons] PRIMARY KEY ([person_id]))
WITH (DATA_COMPRESSION = PAGE)
GO


CREATE TABLE [hhtbs2016].[trips](
	[trip_id] bigint NOT NULL,
	[trip_id_linked] bigint NOT NULL,
	[trip_id_location] bigint NOT NULL,
	[person_id] bigint NOT NULL,
	[household_id] int NOT NULL,
	[travel_date] date NOT NULL,
	[travel_day_number] tinyint NULL,
	[travel_day_of_week] nvarchar(10) NOT NULL,
	[data_source] nvarchar(10) NOT NULL,
	[completed_trip_survey] nvarchar(5) NOT NULL,
	[completed_date] smalldatetime NULL,
	[completed_household_survey] nvarchar(15) NOT NULL,
	[completed_person_survey] nvarchar(15) NOT NULL,
	[number_household_survey_weekdays] tinyint NULL,
	[revised_at] smalldatetime NULL,
	[revised_count] nvarchar(15) NOT NULL,
	[error] nvarchar(20) NOT NULL,
	[flag_teleport] nvarchar(15) NOT NULL,
	[copied_trip] nvarchar(15) NOT NULL,
	[analyst_merged_trip] nvarchar(15) NOT NULL,
	[analyst_split_trip] nvarchar(15) NOT NULL,
	[user_merged_trip] nvarchar(15) NOT NULL,
	[user_split_trip] nvarchar(15) NOT NULL,
	[added_trip] nvarchar(15) NOT NULL,
	[nonproxy_derived_trip] nvarchar(15) NOT NULL,
	[proxy_added_trip] nvarchar(15) NOT NULL,
	[unlinked_transit_trip] nvarchar(5) NOT NULL,
	[origin_name] nvarchar(150) NOT NULL,
	[origin_address] nvarchar(150) NOT NULL,
	[origin_latitude] float NULL,
	[origin_longitude] float NULL,
	[origin_shape] geometry NULL,
	[origin_mgra_13] integer NULL,
	[destination_name] nvarchar(150) NOT NULL,
	[destination_address] nvarchar(150) NOT NULL,
	[destination_latitude] float NULL,
	[destination_longitude] float NULL,
	[destination_shape] geometry NULL,
	[destination_mgra_13] integer NULL,
	[origin_purpose] nvarchar(60) NOT NULL,
	[origin_purpose_other_specify] nvarchar(150) NOT NULL,
	[origin_purpose_inferred] nvarchar(50) NOT NULL,
	[destination_purpose] nvarchar(60) NOT NULL,
	[destination_purpose_other_specify] nvarchar(150) NOT NULL,
	[destination_purpose_inferred] nvarchar(50) NOT NULL,
	[departure_time] smalldatetime NOT NULL,
	[arrival_time] smalldatetime NOT NULL,
	[travelers] nvarchar(25) NOT NULL,
	[travelers_household] nvarchar(25) NOT NULL,
	[travelers_non_household] nvarchar(25) NOT NULL,
	[mode_1] nvarchar(30) NOT NULL,
	[mode_2] nvarchar(30) NOT NULL,
	[mode_3] nvarchar(30) NOT NULL,
	[mode_4] nvarchar(30) NOT NULL,
	[mode_transit_access] nvarchar(35) NOT NULL,
	[mode_transit_egress] nvarchar(35) NOT NULL,
	[google_mode] nvarchar(15) NOT NULL,
	[driver] nvarchar(40) NOT NULL,
	[toll_road] nvarchar(15) NOT NULL,
	[toll_road_express] nvarchar(15) NOT NULL,
	[parking_location] nvarchar(50) NOT NULL,
	[parking_pay_type] nvarchar(50) NOT NULL,
	[parking_cost] float NULL,
	[parking_cost_dk] nvarchar(15) NULL,
	[parking_egress_duration] float NULL,
	[taxi_pay_type] nvarchar(45) NULL,
	[taxi_cost] float NULL,
	[taxi_cost_dk] nvarchar(15) NULL,
	[airplane_pay_type] nvarchar(45) NOT NULL,
	[airplane_cost] float NULL,
	[airplane_cost_dk] nvarchar(15) NULL,
	[bus_pay_type] nvarchar(35) NULL,
	[bus_cost] float NULL,
	[bus_cost_dk] nvarchar(15) NULL,
	[rail_pay_type] nvarchar(35) NULL,
	[rail_cost] float NULL,
	[rail_cost_dk] nvarchar(15) NULL,
	[ferry_pay_type] nvarchar(35) NULL,
	[ferry_cost] float NULL,
	[ferry_cost_dk] nvarchar(15) NULL,
	[parkride_lot] nvarchar(40) NULL,
	[parkride_city] nvarchar(15) NULL,
	[distance] float NULL,
	[duration] float NULL,
	[duration_reported] float NULL,
	[speed] float NULL,
	[weight_trip] float NULL,
	[weight_person_trip] float NULL,
	[weight_household_multiday_factor] float NULL,
	[weight_person_multiday_456x] float NULL,
    CONSTRAINT [pk_hhtbs2016_trips] PRIMARY KEY CLUSTERED ([trip_id]))
WITH (DATA_COMPRESSION = PAGE)
GO


-- household vehicles (autos only)
CREATE TABLE [hhtbs2016].[vehicles] (
	[vehicle_id] integer NOT NULL,
	[household_id] integer NOT NULL,
	[vehicle_number] integer NOT NULL,
    [year] nvarchar(25) NOT NULL,
    [make] nvarchar(75) NOT NULL,
    [model] nvarchar(75) NOT NULL,
    [fuel_type] nvarchar(25) NOT NULL,
    [how_obtained] nvarchar(30) NOT NULL,
    [toll_transponder] nvarchar(25) NOT NULL,
    [residence_parking_pass] nvarchar(75) NOT NULL,
    [residence_parking_cost_unknown] nvarchar(25) NOT NULL,
    [residence_parking_monthly_cost] float NULL,
    CONSTRAINT [pk_hhtbs2016_vehicles] PRIMARY KEY ([vehicle_id]),
    CONSTRAINT [ixuq_hhtbs2016_vehicles] UNIQUE ([household_id], [vehicle_number])
    )
WITH (DATA_COMPRESSION = PAGE)
GO

-- insert NULL record
INSERT INTO [hhtbs2016].[vehicles] VALUES
(0, 0, 0, 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable', NULL)
