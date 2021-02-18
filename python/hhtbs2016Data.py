# -*- coding: utf-8 -*-
""" Household Travel Behavior Survey 2016 Data Module.

This module contains classes holding all information and utilities relating to
the delivered data-sets for the 2016 Household Travel Behavior Survey. This
module is used to create data-sets from the delivered data-sets that are
suitable for reporting purposes, analyst consumption, and database constructs.

Notes:
    docstring style guide - http://google.github.io/styleguide/pyguide.html
"""

from functools import lru_cache  # caching decorator for modules
import numpy as np
import pandas as pd
import pyproj
from shapely import geometry, ops
from typing import Iterable


class SurveyData(object):
    """ This is the parent class for all information and utilities relating
    to the 2016 Household Travel Behavior Survey data-sets.

    Methods:
        frequencies: Method to create diagnostic frequency tables used in
            the household travel survey GitHub Wiki
        line_wkt: Method to create WKT string representations of lines
            with vertices in latitude/longitude coordinates using a
            given input crs
        point_wkt: Method to create WKT string representations of
            latitude/longitude coordinates using a given input crs

    Properties:
        border_trips: household border trip list
        day: person trip list day-level data
        households: household list
        intercept: AT intercept survey
        location: trip list travel path geometries
        persons: person list
        trips: trip list
        vehicles: household vehicle list

 """

    def __init__(self) -> None:
        pass

    @staticmethod
    def frequencies(df: pd.DataFrame, user_missing: list) -> dict:
        """ Run diagnostic frequency tables for visual inspection. Output in
        list format for use with Python tabulate library. Used to populate
        GitHub Wiki.

        Args:
            df: A Pandas DataFrame
            user_missing: List of string values to denote as missing values
                for category data type columns

        Returns:
            A dictionary containing two list elements;
                n: A list of lists, one for each column of the input Pandas
                    DataFrame containing four elements;
                        column name
                        count of non-missing records
                        count of missing records
                        percentage of missing records
                freq: A list of lists, one for each category data type column
                    of the input Pandas DataFrame. Each list contains lists,
                    one for each unique value of the category data type column
                    containing three elements;
                        unique value of the category data type column
                        count of records
                        percentage of records """

        # initialize return lists
        n_list = []
        values_list = []

        # for each field in the input DataFrame
        for field in list(df):
            col = df[field]

            # if field is a category data type
            if col.dtype.name == "category":
                # calculate n and missing counts incorporating user input missing values
                n = col.count() - col.isin(user_missing).sum()
                m = col.isnull().sum() + col.isin(user_missing).sum()

                # create frequency table of unique values and append to return list
                values = [[field, "", ""]]
                counts = col.value_counts().reindex(col.cat.categories.values)
                pct = (100 * col.value_counts(normalize=True).reindex(col.cat.categories.values)).round(1)
                for i, j, k in zip(counts.index.values, counts.values, pct.values):
                    values.append([i, j, k])
                values_list.append(values)
            else:
                n = col.count()
                m = col.isnull().sum()

            n_list.append([field, n, m, (100 * m / (n + m)).round(1)])

        return {"n": n_list, "freq": values_list}

    @staticmethod
    def line_wkt(lines: Iterable, crs: str) -> list:
        """ Create WKT representation of line geometry for each element of
        ordered coordinates projected to a given crs. It is assumed the line
        cannot pass through the same vertex multiple times. Duplicate vertices
        are dropped, keeping the first instance of the vertex.

        Args:
            lines: An iterable of iterables containing ordered
                (longitude, latitude) tuples defining vertices of a line
                [ [(lng, lat), (lng, lat), (lng, lat)], [(lng, lat), (lng, lat)] ]
            crs: A string specifying the desired Coordinate Reference System
                of the return line geometries (see pyproj.CRS)

        Returns:
            wkts: A list of WKT representations of line geometries """

        # initialize empty return list to hold WKT representations
        wkts = []

        # create transformer of lat/long coordinate system to input crs
        transformer = pyproj.Transformer.from_crs(
            pyproj.CRS("EPSG:4326"),
            pyproj.CRS(crs),
            always_xy=True)

        # for each iterable of ordered (longitude, latitude) tuples
        # create the Shapely LineString geometry and transform to input crs
        # store WKT representation of geometry in return list
        for line in lines:
            if len(line) > 0:
                # remove duplicate points (if any) retaining order
                points = []
                [points.append(x) for x in line if x not in points]

                if len(points) >= 2:
                    value = ops.transform(
                        transformer.transform,
                        geometry.LineString([geometry.Point(xy) for xy in points])
                    )
                elif len(points) == 1:
                    value = ops.transform(
                        transformer.transform,
                        geometry.Point(points[0])
                    )
                else:
                    value = None

                if value.is_valid:
                    wkts.append(value.wkt)
                else:
                    wkts.append(None)
            else:
                wkts.append(None)

        return wkts

    @staticmethod
    def point_wkt(coordinates: Iterable, crs: str) -> list:
        """ Create WKT representation of point geometry from coordinates
        projected to a given crs.

        Args:
            coordinates: An iterable of (longitude, latitude) tuples
                [(lng, lat), (lng, lat), (lng, lat)]]
            crs: A string specifying the desired Coordinate Reference System
                of the return point geometries (see pyproj.CRS)

        Returns:
            wkts: A list of WKT representations of point geometries """

        # initialize empty return list to hold WKT representations
        wkts = []

        # create transformer of lat/long coordinate system to input crs
        transformer = pyproj.Transformer.from_crs(
            pyproj.CRS("EPSG:4326"),
            pyproj.CRS(crs),
            always_xy=True)

        # for each (longitude, latitude) tuple
        # create the Shapely Point geometry and transform to input crs
        # store WKT representation of geometry in return list
        for point in [geometry.Point(xy) for xy in coordinates]:
            value = ops.transform(transformer.transform, point)
            if value.is_valid:
                wkts.append(value.wkt)
            else:
                wkts.append(np.NaN)

        return wkts

    @property
    @lru_cache(maxsize=1)
    def border_trips(self) -> pd.DataFrame:
        """ Household Cross Border Trip list containing the following columns:
                border_trip_id - unique surrogate key
                household_id - unique identifier of household
                trip_id - unique trip id within household
                mode - mode of cross border trip
                port_of_entry - cross border point of entry
                purpose - purpose of cross border trip
                duration - duration of cross border trip
                party_size - party size of cross border trip """

        # combine HHTBS and AT data-sets
        df = pd.concat(
            [
                pd.read_csv("../data/sdrts/SDRTS_Household_Data_20170731.csv",
                            usecols=["hhid",
                                     "border_mode_1",
                                     "border_poe_1",
                                     "border_purpose_1",
                                     "border_duration_1",
                                     "border_party_1",
                                     "border_mode_2",
                                     "border_poe_2",
                                     "border_purpose_2",
                                     "border_duration_2",
                                     "border_party_2",
                                     "border_mode_3",
                                     "border_poe_3",
                                     "border_purpose_3",
                                     "border_duration_3",
                                     "border_party_3",
                                     "border_mode_4",
                                     "border_poe_4",
                                     "border_purpose_4",
                                     "border_duration_4",
                                     "border_party_4"],
                            dtype={"border_mode_1": "Int8",
                                   "border_poe_1": "Int8",
                                   "border_purpose_1": "Int8",
                                   "border_duration_1": "Int8",
                                   "border_party_1": "Int8",
                                   "border_mode_2": "Int8",
                                   "border_poe_2": "Int8",
                                   "border_purpose_2": "Int8",
                                   "border_duration_2": "Int8",
                                   "border_party_2": "Int8",
                                   "border_mode_3": "Int8",
                                   "border_poe_3": "Int8",
                                   "border_purpose_3": "Int8",
                                   "border_duration_3": "Int8",
                                   "border_party_3": "Int8",
                                   "border_mode_4": "Int8",
                                   "border_poe_4": "Int8",
                                   "border_purpose_4": "Int8",
                                   "border_duration_4": "Int8",
                                   "border_party_4": "Int8"}),
                pd.read_csv("../data/at/SDRTS_AT_HH_Data_20170809.csv",
                            usecols=["hhid",
                                     "border_mode_1",
                                     "border_poe_1",
                                     "border_purpose_1",
                                     "border_duration_1",
                                     "border_party_1",
                                     "border_mode_2",
                                     "border_poe_2",
                                     "border_purpose_2",
                                     "border_duration_2",
                                     "border_party_2",
                                     "border_mode_3",
                                     "border_poe_3",
                                     "border_purpose_3",
                                     "border_duration_3",
                                     "border_party_3",
                                     "border_mode_4",
                                     "border_poe_4",
                                     "border_purpose_4",
                                     "border_duration_4",
                                     "border_party_4"],
                            dtype={"border_mode_1": "Int8",
                                   "border_poe_1": "Int8",
                                   "border_purpose_1": "Int8",
                                   "border_duration_1": "Int8",
                                   "border_party_1": "Int8",
                                   "border_mode_2": "Int8",
                                   "border_poe_2": "Int8",
                                   "border_purpose_2": "Int8",
                                   "border_duration_2": "Int8",
                                   "border_party_2": "Int8",
                                   "border_mode_3": "Int8",
                                   "border_poe_3": "Int8",
                                   "border_purpose_3": "Int8",
                                   "border_duration_3": "Int8",
                                   "border_party_3": "Int8",
                                   "border_mode_4": "Int8",
                                   "border_poe_4": "Int8",
                                   "border_purpose_4": "Int8",
                                   "border_duration_4": "Int8",
                                   "border_party_4": "Int8"})
            ],
            ignore_index=True
        )

        # reshape data-set from wide to long
        df = pd.wide_to_long(
            df=df,
            stubnames=["border_mode",
                       "border_poe",
                       "border_purpose",
                       "border_duration",
                       "border_party"],
            sep="_",
            i="hhid",
            j="trip_id"
        )

        # apply exhaustive field mappings where applicable
        mappings = {
            "border_mode": {1: "My own vehicle (or motorcycle)",
                            2: "Other vehicle (e.g., rental, carshare, taxi, work car, friends)",
                            3: "Bus/shuttle",
                            4: "Walking (or biking)",
                            5: "Airplane (or helicopter)",
                            97: "Other way of traveling"},
            "border_poe": {1: "Otay Mesa (SR-905) Port of Entry",
                           2: "San Ysidro (I-5/I-805) Port of Entry",
                           3: "Tecate (SR 188) Port of Entry",
                           4: "Cross-border Terminal, Tijuana Intl Airport (pedestrian only)",
                           97: "Other"},
            "border_purpose": {1: "Drop-off/pick-up someone (e.g., at Tijuana Intl Airport)",
                               2: "Social (visit friends/family)",
                               3: "Leisure/recreation/vacation",
                               4: "Work/business-related",
                               5: "Personal business (e.g., medical appointment)",
                               97: "Other"},
            "border_duration": {1: "Less than 1 day",
                                2: "1-2 days",
                                3: "3-5 days",
                                4: "6-10 days",
                                5: "More than 10 days"},
            "border_party": {1: "1 (I traveled alone)",
                             2: "2 persons total",
                             3: "3 persons total",
                             4: "4 persons total",
                             5: "5 or more persons total (including me)"}
        }

        # set categorical variable data types after wide_to_long/melt
        # as wide_to_long/melt removes pd.Categorical data type
        for field in mappings:
            # define using pd.Categorical to maintain defined category order
            # without setting ordered parameter to True
            df[field] = pd.Categorical(
                df[field].map(mappings[field]),
                categories=mappings[field].values()
            )

        # drop empty placeholder records
        df.dropna(inplace=True)

        # add surrogate key
        df["border_trip_id"] = np.arange(len(df))

        # move multi-index into columns
        df.reset_index(level=df.index.names, inplace=True)

        # rename columns
        df.rename(columns={
            "hhid": "household_id",
            "border_mode": "mode",
            "border_poe": "port_of_entry",
            "border_purpose": "purpose",
            "border_duration": "duration",
            "border_party": "party_size"
        },
            inplace=True)

        return df[["border_trip_id",
                   "household_id",
                   "trip_id",
                   "mode",
                   "port_of_entry",
                   "purpose",
                   "duration",
                   "party_size"]]

    @property
    @lru_cache(maxsize=1)
    def day(self) -> pd.DataFrame:
        """ Person day-level trip diary data-set containing the following
        columns:
                day_id - unique surrogate key
                person_id - unique identifier of person
                household_id - unique identifier of household
                travel_date - date of travel
                travel_day_number - travel day number for person (1-7)
                travel_day_of_week - travel day of week
                data_source - trip was recorded by rMove or online
                completed_household_survey - all household surveys completed on travel date of trip
                completed_person_survey - all surveys completed by person on travel date of trip
                completed_date - rMove trip; date and time survey was completed
                revised_at - rMove trip; Date and time survey was last revised
                revised_count - rMove trip; Number of revisions to survey
                diary_start_time - online diary; time started diary
                diary_end_time - online diary; time ended diary
                diary_duration - online diary; minutes spend taking online diary survey
                survey_status - diary/summary survey completion status
                proxy - online diary; completed by proxy
                made_trips - made trips on travel date
                no_trips_reason_1 - no trips; reason for no trips on travel date
                no_trips_reason_2 - no trips; reason for no trips on travel date
                no_trips_reason_specify_other - no trips; specify other reason for no trips
                    on travel date
                number_trips - number of unlinked trips on travel date
                number_surveys - number of trip surveys completed on travel date
                start_location - online diary; location at 3am on travel date
                start_location_other - online diary; specify other location at 3am on travel date
                end_location - online diary; location at 3am on day after travel date
                end_location_other - online diary; specify other location at 3am on day after travel date
                time_telework - employed/volunteer (non-active duty or deployed),
                    all rMoves, 18+ online diary; time spent teleworking on travel date (hours)
                time_shop - all rMoves, 18+ online diary; time spent shopping online
                    on travel date (hours)
                toll_road - rMove diary; used toll road
                toll_road_express - rMove diary; used toll road with express lane
                deliver_package - all rMoves, first adult answer online diary; received
                    mailed packages on travel date
                deliver_food - all rMoves, first adult answer online diary; had food
                    delivered on travel date
                deliver_work - all rMoves, first adult answer online diary; someone came
                    to house to do service/work on travel date
                weight_household_multiday_factor - household multi-day weight factor
                weight_person_multiday_456x - factored person-day weight """

        # load AT data-set
        at_df = pd.read_csv(
                    "../data/at/SDRTS_AT_Day_Data_20170831.csv",
                    usecols=["personid",
                             "hhid",
                             "traveldate",
                             "daynum",
                             "travel_dow",
                             # "data_source",
                             # "day_hhcomplete",
                             "day_iscomplete",
                             "completed_at",
                             "revised_at",
                             "revised_count",
                             # "diary_start_pt",
                             # "diary_end_pt",
                             # "diary_duration",
                             "survey_status",
                             # "proxy",
                             "trips_yesno",
                             "notravel",
                             "notravel_secondary",
                             # "notravel_other",
                             "num_trips",
                             "num_answer",
                             # "loc_start",
                             # "loc_start_other",
                             # "loc_end",
                             # "loc_end_other",
                             "telework_time",
                             "shop_time",
                             "toll_no",
                             "toll_express",
                             "deliver_package",
                             "deliver_food",
                             "deliver_work"
                             # "multiday_factor",
                             # "multiday_weight_456x"
                             ],
                    dtype={"travel_dow": "Int8",
                           # "data_source": "Int8",
                           # "day_hhcomplete": "Int8",
                           "day_iscomplete": "Int8",
                           "revised_count": "Int8",
                           "diary_duration": "Int8",
                           "survey_status": "Int8",
                           "proxy": "Int8",
                           "trips_yesno": "Int8",
                           "notravel": "Int16",
                           "notravel_secondary": "Int16",
                           # "loc_start": "Int8",
                           # "loc_end": "Int8",
                           "toll_no": "Int16",
                           "toll_express": "Int16",
                           "deliver_package": "Int16",
                           "deliver_food": "Int16",
                           "deliver_work": "Int16"}
                )

        # set values for variables not in AT data-set
        at_df["data_source"] = 1  # all use rMoves
        at_df["day_hhcomplete"] = 99  # new hardcoded Not Applicable value
        at_df["notravel_other"] = "Not Applicable"
        at_df["loc_start"] = 99  # new hardcoded Not Applicable value
        at_df["loc_start_other"] = "Not Applicable"
        at_df["loc_end"] = 99  # new hardcoded Not Applicable value
        at_df["loc_end_other"] = "Not Applicable"

        # combine HHTBS and AT data-sets
        df = pd.concat(
            [
                pd.read_csv(
                    "../data/sdrts/SDRTS_Day_Data_20170731.csv",
                    usecols=["personid",
                             "hhid",
                             "traveldate",
                             "daynum",
                             "travel_dow",
                             "data_source",
                             "day_hhcomplete",
                             "day_iscomplete",
                             "completed_at",
                             "revised_at",
                             "revised_count",
                             "diary_start_pt",
                             "diary_end_pt",
                             "diary_duration",
                             "survey_status",
                             "proxy",
                             "trips_yesno",
                             "notravel",
                             "notravel_secondary",
                             "notravel_other",
                             "num_trips",
                             "num_answer",
                             "loc_start",
                             "loc_start_other",
                             "loc_end",
                             "loc_end_other",
                             "telework_time",
                             "shop_time",
                             "toll_no",
                             "toll_express",
                             "deliver_package",
                             "deliver_food",
                             "deliver_work",
                             "multiday_factor",
                             "multiday_weight_456x"],
                    dtype={"travel_dow": "Int8",
                           "data_source": "Int8",
                           "day_hhcomplete": "Int8",
                           "day_iscomplete": "Int8",
                           "revised_count": "Int8",
                           "diary_duration": "Int8",
                           "survey_status": "Int8",
                           "proxy": "Int8",
                           "trips_yesno": "Int8",
                           "notravel": "Int16",
                           "notravel_secondary": "Int16",
                           "loc_start": "Int8",
                           "loc_end": "Int8",
                           "toll_no": "Int16",
                           "toll_express": "Int16",
                           "deliver_package": "Int16",
                           "deliver_food": "Int16",
                           "deliver_work": "Int16"}
                ),
                at_df
            ],
            ignore_index=True
        )

        # apply exhaustive field mappings where applicable
        mappings = {
            "travel_dow": {1: "Monday",
                           2: "Tuesday",
                           3: "Wednesday",
                           4: "Thursday",
                           5: "Friday",
                           6: "Saturday",
                           7: "Sunday",
                           pd.NA: "Missing"},
            "data_source": {1: "rMove",
                            2: "Online",
                            pd.NA: "Missing"},
            "day_hhcomplete": {0: "No",
                               1: "Yes",
                               99: "Not Applicable",
                               pd.NA: "Missing"},
            "day_iscomplete": {0: "No",
                               1: "Yes",
                               pd.NA: "Missing"},
            "survey_status": {0: "Diary/daily summary survey not complete",
                              1: "Diary/daily summary survey complete",
                              2: "Diary/daily summary survey not asked",
                              pd.NA: "Not Applicable"},
            "proxy": {1: "No",
                      2: "Present while other member filled out survey",
                      3: "Not present while other member filled out survey",
                      pd.NA: "Not Applicable"},
            "trips_yesno": {1: "Yes",
                            2: "No",
                            pd.NA: "Missing"},
            "notravel": {1: "Did travel, but rMove did not collect any trips",
                         2: "Not scheduled to work/took day off",
                         3: "Worked at home (for pay)",
                         4: "Worked around home (not for pay)",
                         5: "Kids were on school vacation/break",
                         6: "No available transportation",
                         7: "Was sick or caring for another person",
                         8: "Was waiting for visitor/delivery",
                         9: "Other reason (no reason given)",
                         10: "Stayed on base all day",
                         97: "Other reason",
                         -9998: "Participant non-response",
                         -9999: "Technical error"},
            "notravel_secondary": {1: "Did travel, but rMove did not collect any trips",
                                   2: "Not scheduled to work/took day off",
                                   3: "Worked at home (for pay)",
                                   4: "Worked around home (not for pay)",
                                   5: "Kids were on school vacation/break",
                                   6: "No available transportation",
                                   7: "Was sick or caring for another person",
                                   8: "Was waiting for visitor/delivery",
                                   9: "Other reason (no reason given)",
                                   10: "Stayed on base all day",
                                   97: "Other reason",
                                   -9998: "Participant non-response",
                                   -9999: "Technical error",
                                   pd.NA: "Not Applicable"},
            "loc_start": {1: "Home",
                          2: "Work (Primary)",
                          3: "Work (Second Jobs)",
                          97: "Other",
                          99: "Not Applicable"},
            "loc_end": {1: "Home",
                        2: "Work (Primary)",
                        3: "Work (Second Jobs)",
                        97: "Other",
                        99: "Not Applicable"},
            "toll_no": {0: "Yes",  # flip mapping from 0-No, 1-Yes
                        1: "No",
                        -9998: "Participant non-response",
                        -9999: "Technical error",
                        pd.NA: "Not Applicable"},
            "toll_express": {0: "No",
                             1: "Yes",
                             -9998: "Participant non-response",
                             -9999: "Technical error",
                             pd.NA: "Not Applicable"},
            "deliver_package": {0: "No",
                                1: "Yes",
                                -9998: "Participant non-response",
                                -9999: "Technical error",
                                pd.NA: "Not Applicable"},
            "deliver_food": {0: "No",
                             1: "Yes",
                             -9998: "Participant non-response",
                             -9999: "Technical error",
                             pd.NA: "Not Applicable"},
            "deliver_work": {0: "No",
                             1: "Yes",
                             -9998: "Participant non-response",
                             -9999: "Technical error",
                             pd.NA: "Not Applicable"}
        }

        for field in mappings:
            # define using pd.Categorical to maintain defined category order
            # without setting ordered parameter to True
            df[field] = pd.Categorical(
                df[field].map(mappings[field]),
                categories=mappings[field].values()
            )

        # enforce agreement between number of trips
        # indicator if trips were made
        # number of trips takes precedence
        df.loc[df.num_trips > 0, "trips_yesno"] = "Yes"
        df.loc[df.num_trips == 0, "trips_yesno"] = "No"

        # add categories to reason for not traveling for upcoming recodes
        df["notravel"].cat.add_categories(["Missing", "Not Applicable"], inplace=True)

        # if no trips made set blank reason for not traveling to Missing
        df.loc[(df.num_trips == 0) & (df.notravel.isnull()),
               "notravel"] = "Missing"

        # if trips made set blank reason for not traveling to Not Applicable
        df.loc[(df.num_trips > 0) & (df.notravel.isnull()),
               "notravel"] = "Not Applicable"

        # if neither variable providing reason for not traveling is Other
        # then set notravel_other to Not Applicable
        df.loc[(df.notravel != "Other reason") &
               (df.notravel_secondary != "Other reason"),
               "notravel_other"] = "Not Applicable"

        # if either variable providing reason for not traveling is Other
        # and notravel_other is missing then set to Missing
        df.loc[((df.notravel == "Other reason") |
                (df.notravel_secondary == "Other reason")) &
               (df.notravel_other.isnull()),
               "notravel_other"] = "Missing"

        # add categories to location start/end for upcoming recodes
        df["loc_start"].cat.add_categories(["Missing"], inplace=True)
        df["loc_end"].cat.add_categories(["Missing"], inplace=True)

        # if rMoves then set all location variables to Not Applicable
        df.loc[df.data_source == "rMove",
               ["loc_start",
                "loc_start_other",
                "loc_end",
                "loc_end_other"]] = "Not Applicable"

        # if online diary set missing location start/end to Missing
        df.loc[(df.data_source == "Online") & (df.loc_start.isnull()),
               "loc_start"] = "Missing"

        df.loc[(df.data_source == "Online") & (df.loc_end.isnull()),
               "loc_end"] = "Missing"

        # if online diary and location start/end != Other then set
        # location start/end specify other reason variable to Not Applicable
        df.loc[(df.data_source == "Online") & (df.loc_start != "Other"),
               "loc_start_other"] = "Not Applicable"

        df.loc[(df.data_source == "Online") & (df.loc_end != "Other"),
               "loc_end_other"] = "Not Applicable"

        # add surrogate key
        df["day_id"] = np.arange(len(df))

        # rename columns
        df.rename(columns={
            "personid": "person_id",
            "hhid": "household_id",
            "traveldate": "travel_date",
            "daynum": "travel_day_number",
            "travel_dow": "travel_day_of_week",
            "day_hhcomplete": "completed_household_survey",
            "day_iscomplete": "completed_person_survey",
            "completed_at": "completed_date",
            "revised_at": "revised_at",
            "diary_start_pt": "diary_start_time",
            "diary_end_pt": "diary_end_time",
            "survey_status": "survey_status",
            "proxy": "proxy",
            "trips_yesno": "made_trips",
            "notravel": "no_trips_reason_1",
            "notravel_secondary": "no_trips_reason_2",
            "notravel_other": "no_trips_reason_specify_other",
            "num_trips": "number_trips",
            "num_answer": "number_surveys",
            "loc_start": "start_location",
            "loc_start_other": "start_location_other",
            "loc_end": "end_location",
            "loc_end_other": "end_location_other",
            "telework_time": "time_telework",
            "shop_time": "time_shop",
            "toll_no": "toll_road",
            "toll_express": "toll_road_express",
            "multiday_factor": "weight_household_multiday_factor",
            "multiday_weight_456x": "weight_person_multiday_456x"
        },
            inplace=True)

        return df[["day_id",
                   "person_id",
                   "household_id",
                   "travel_date",
                   "travel_day_number",
                   "travel_day_of_week",
                   "data_source",
                   "completed_household_survey",
                   "completed_person_survey",
                   "completed_date",
                   "revised_at",
                   "revised_count",
                   "diary_start_time",
                   "diary_end_time",
                   "diary_duration",
                   "survey_status",
                   "proxy",
                   "made_trips",
                   "no_trips_reason_1",
                   "no_trips_reason_2",
                   "no_trips_reason_specify_other",
                   "number_trips",
                   "number_surveys",
                   "start_location",
                   "start_location_other",
                   "end_location",
                   "end_location_other",
                   "time_telework",
                   "time_shop",
                   "toll_road",
                   "toll_road_express",
                   "deliver_package",
                   "deliver_food",
                   "deliver_work",
                   "weight_household_multiday_factor",
                   "weight_person_multiday_456x"]]

    @property
    @lru_cache(maxsize=1)
    def households(self) -> pd.DataFrame:
        """ Household list containing the following columns:
                household_id - unique identifier of household
                sample_segment - sample segment name
                sample_group - sample participation group
                travel_date_start - assigned travel date
                recruit_survey_where - where recruit survey was completed
                recruit_survey_mobile - recruit survey was completed on mobile device
                recruit_survey_start - recruit survey start time PST
                recruit_survey_end - recruit survey end time PST
                number_rmove_participants - number of rMove participants in household
                participate_future_studies - willing to participate in future studies
                household_completed - household has one day where all surveys are answered
                completed_days - number of days where all surveys answered by household
                language - preferred language for future communications
                language_other - specify other language if selected
                persons - number of household members
                adults - number of household adults
                children - number of household children
                workers - number of household workers
                vehicles - number of household vehicles
                bicycles - number of household bicycles
                has_share_car - household belongs to a car share
                has_share_bicycle - household belongs to a bike share
                has_share_vanpool - household belongs to a van-pool
                address - home address
                latitude - home location latitude
                longitude - home location longitude
                shape - home location shape attribute (EPSG: 2230)
                residence_duration - duration at current residence
                residence_type - type of residence
                income_category_detailed - previous year household income detailed categories
                income_category_broad - previous year household income broad categories
                use_paper_maps - household uses paper maps for trip planning
                freq_paper_maps - frequency household uses paper maps for trip planning
                use_car_navigation - household uses car navigation for trip planning
                freq_car_navigation - frequency household uses car navigation for trip planning
                use_511sd - household uses 511sd for trip planning
                freq_511sd - frequency household uses 511sd for trip planning
                use_apple_maps - household uses apple maps for trip planning
                freq_apple_maps - frequency household uses apple maps for trip planning
                use_car2go - household uses car2go for trip planning
                freq_car2go - frequency household uses car2go for trip planning
                use_google_maps - household uses google maps for trip planning
                freq_google_maps - frequency household uses google maps for trip planning
                use_icommutesd - household uses iCommuteSD for trip planning
                freq_icommutesd - frequency household uses iCommuteSD for trip planning
                use_lyft - household uses Lyft for trip planning
                freq_lyft - frequency household uses Lyft for trip planning
                use_mapmyride - household uses MapMyRide for trip planning
                freq_mapmyride - frequency household uses MapMyRide for trip planning
                use_mapquest - household uses MapQuest for trip planning
                freq_mapquest - frequency household uses MapQuest for trip planning
                use_sdmts - household uses SD MTS for trip planning
                freq_sdmts - frequency household uses SD MTS for trip planning
                use_nctd - household uses NCTD for trip planning
                freq_nctd - frequency household uses NCTD for trip planning
                use_waze - household uses Waze for trip planning
                freq_waze - frequency household uses Waze for trip planning
                use_uber - household uses Uber for trip planning
                freq_uber - frequency household uses Uber for trip planning
                use_other_tool - household uses other tool for trip planning
                specify_other_tool - specify other tool household uses for trip planning
                freq_other_tool - frequency household uses other tool for trip planning
                use_no_navigation_tools - household uses no navigation tools
                freq_cross_border - frequency household crosses the border
                weight_household_initial - initial household expansion weight
                weight_household_4x - household weight with maximum of 4x initial weight
                weight_household_456x - household weight recommended for use """

        # load AT data-set
        at_df = pd.read_csv(
            "../data/at/SDRTS_AT_HH_Data_20170809.csv",
            usecols=["hhid",
                     # "sample_segment",
                     "hhgroup",
                     "traveldate_start",
                     # "callcenter_recruit",
                     "mobile_recruit",
                     "recruit_start_pt",
                     "recruit_end_pt",
                     "num_rmove_participants",
                     "participate_future_studies",
                     "hh_iscomplete",
                     "numdays_complete",
                     "language_pref",
                     "language_pref_other",
                     "hhsize",
                     "numadults",
                     "numkids",
                     "numworkers",
                     "vehicle_count",
                     "bicycle_count",
                     "share_car",
                     "share_bike",
                     "share_vanpool",
                     "home_address",
                     "home_lat",
                     "home_lng",
                     "res_duration",
                     "rent_own",
                     "res_type",
                     "hhincome_detailed",
                     "hhincome_broad",
                     "tool_paper",
                     "tool_freq_paper",
                     "tool_carnav",
                     "tool_freq_carnav",
                     "tool_511sd",
                     "tool_freq_511sd",
                     "tool_apple",
                     "tool_freq_apple",
                     "tool_car2go",
                     "tool_freq_car2go",
                     "tool_google",
                     "tool_freq_google",
                     "tool_icommutesd",
                     "tool_freq_icommutesd",
                     "tool_lyft",
                     "tool_freq_lyft",
                     "tool_mapmy",
                     "tool_freq_mapmy",
                     "tool_mapquest",
                     "tool_freq_mapquest",
                     "tool_sdmts",
                     "tool_freq_sdmts",
                     "tool_nctd",
                     "tool_freq_nctd",
                     "tool_waze",
                     "tool_freq_waze",
                     "tool_uber",
                     "tool_freq_uber",
                     "tool_other",
                     "tool_other_specify",
                     "tool_freq_other",
                     "tool_none",
                     "border_freq_1"
                     # "hh_init_wt",
                     # "hh_weight_4x",
                     # "hh_final_weight_456x"]
                     ],
            dtype={"hhgroup": "Int8",
                   # "callcenter_recruit": "Int8",
                   "mobile_recruit": "Int8",
                   "participate_future_studies": "Int8",
                   "hh_iscomplete": "Int8",
                   "language_pref": "Int8",
                   "hhsize": "Int8",
                   "numadults": "Int8",
                   "numkids": "Int8",
                   "numworkers": "Int8",
                   "vehicle_count": "Int8",
                   "bicycle_count": "Int8",
                   "share_car": "Int8",
                   "share_bike": "Int8",
                   "share_vanpool": "Int8",
                   "res_duration": "Int8",
                   "rent_own": "Int8",
                   "res_type": "Int8",
                   "hhincome_detailed": "Int8",
                   "hhincome_broad": "Int8",
                   "tool_paper": "Int8",
                   "tool_freq_paper": "Int8",
                   "tool_carnav": "Int8",
                   "tool_freq_carnav": "Int8",
                   "tool_511sd": "Int8",
                   "tool_freq_511sd": "Int8",
                   "tool_apple": "Int8",
                   "tool_freq_apple": "Int8",
                   "tool_car2go": "Int8",
                   "tool_freq_car2go": "Int8",
                   "tool_google": "Int8",
                   "tool_freq_google": "Int8",
                   "tool_icommutesd": "Int8",
                   "tool_freq_icommutesd": "Int8",
                   "tool_lyft": "Int8",
                   "tool_freq_lyft": "Int8",
                   "tool_mapmy": "Int8",
                   "tool_freq_mapmy": "Int8",
                   "tool_mapquest": "Int8",
                   "tool_freq_mapquest": "Int8",
                   "tool_sdmts": "Int8",
                   "tool_freq_sdmts": "Int8",
                   "tool_nctd": "Int8",
                   "tool_freq_nctd": "Int8",
                   "tool_waze": "Int8",
                   "tool_freq_waze": "Int8",
                   "tool_uber": "Int8",
                   "tool_freq_uber": "Int8",
                   "tool_other": "Int8",
                   "tool_freq_other": "Int8",
                   "tool_none": "Int8",
                   "border_freq_1": "Int8"}
        )

        # set values for variables not in AT data-set
        at_df["sample_segment"] = "AT segment"
        at_df["callcenter_recruit"] = 99  # new hardcoded Not Applicable value

        # combine HHTBS and AT data-sets
        df = pd.concat(
            [
                pd.read_csv(
                    "../data/sdrts/SDRTS_Household_Data_20170731.csv",
                    usecols=["hhid",
                             "sample_segment",
                             "hhgroup",
                             "traveldate_start",
                             "callcenter_recruit",
                             "mobile_recruit",
                             "recruit_start_pt",
                             "recruit_end_pt",
                             "num_rmove_participants",
                             "participate_future_studies",
                             "hh_iscomplete",
                             "numdays_complete",
                             "language_pref",
                             "language_pref_other",
                             "hhsize",
                             "numadults",
                             "numkids",
                             "numworkers",
                             "vehicle_count",
                             "bicycle_count",
                             "share_car",
                             "share_bike",
                             "share_vanpool",
                             "home_address",
                             "home_lat",
                             "home_lng",
                             "res_duration",
                             "rent_own",
                             "res_type",
                             "hhincome_detailed",
                             "hhincome_broad",
                             "tool_paper",
                             "tool_freq_paper",
                             "tool_carnav",
                             "tool_freq_carnav",
                             "tool_511sd",
                             "tool_freq_511sd",
                             "tool_apple",
                             "tool_freq_apple",
                             "tool_car2go",
                             "tool_freq_car2go",
                             "tool_google",
                             "tool_freq_google",
                             "tool_icommutesd",
                             "tool_freq_icommutesd",
                             "tool_lyft",
                             "tool_freq_lyft",
                             "tool_mapmy",
                             "tool_freq_mapmy",
                             "tool_mapquest",
                             "tool_freq_mapquest",
                             "tool_sdmts",
                             "tool_freq_sdmts",
                             "tool_nctd",
                             "tool_freq_nctd",
                             "tool_waze",
                             "tool_freq_waze",
                             "tool_uber",
                             "tool_freq_uber",
                             "tool_other",
                             "tool_other_specify",
                             "tool_freq_other",
                             "tool_none",
                             "border_freq_1",
                             "hh_init_wt",
                             "hh_weight_4x",
                             "hh_final_weight_456x"],
                    dtype={"hhgroup": "Int8",
                           "callcenter_recruit": "Int8",
                           "mobile_recruit": "Int8",
                           "participate_future_studies": "Int8",
                           "hh_iscomplete": "Int8",
                           "language_pref": "Int8",
                           "hhsize": "Int8",
                           "numadults": "Int8",
                           "numkids": "Int8",
                           "numworkers": "Int8",
                           "vehicle_count": "Int8",
                           "bicycle_count": "Int8",
                           "share_car": "Int8",
                           "share_bike": "Int8",
                           "share_vanpool": "Int8",
                           "res_duration": "Int8",
                           "rent_own": "Int8",
                           "res_type": "Int8",
                           "hhincome_detailed": "Int8",
                           "hhincome_broad": "Int8",
                           "tool_paper": "Int8",
                           "tool_freq_paper": "Int8",
                           "tool_carnav": "Int8",
                           "tool_freq_carnav": "Int8",
                           "tool_511sd": "Int8",
                           "tool_freq_511sd": "Int8",
                           "tool_apple": "Int8",
                           "tool_freq_apple": "Int8",
                           "tool_car2go": "Int8",
                           "tool_freq_car2go": "Int8",
                           "tool_google": "Int8",
                           "tool_freq_google": "Int8",
                           "tool_icommutesd": "Int8",
                           "tool_freq_icommutesd": "Int8",
                           "tool_lyft": "Int8",
                           "tool_freq_lyft": "Int8",
                           "tool_mapmy": "Int8",
                           "tool_freq_mapmy": "Int8",
                           "tool_mapquest": "Int8",
                           "tool_freq_mapquest": "Int8",
                           "tool_sdmts": "Int8",
                           "tool_freq_sdmts": "Int8",
                           "tool_nctd": "Int8",
                           "tool_freq_nctd": "Int8",
                           "tool_waze": "Int8",
                           "tool_freq_waze": "Int8",
                           "tool_uber": "Int8",
                           "tool_freq_uber": "Int8",
                           "tool_other": "Int8",
                           "tool_freq_other": "Int8",
                           "tool_none": "Int8",
                           "border_freq_1": "Int8"}
                ),
                at_df
            ],
            ignore_index=True
        )

        # apply exhaustive field mappings where applicable
        mappings = {
            "sample_segment": {"AT segment": "AT segment",
                               "Hispanic oversample": "Hispanic oversample",
                               "Other oversample": "Other oversample",
                               "Regular": "Regular",
                               "Transportation oversample": "Transportation oversample"},
            "hhgroup": {1: "Group 1: rMove only",
                        3: "Group 3: Online diary only",
                        4: "Group 4: Split HH: some rMove, some online diary",
                        5: "Group 5: AT/Intercept (rMove only)",
                        pd.NA: "Missing"},
            "callcenter_recruit": {0: "Recruit survey completed online",
                                   1: "Recruit survey completed via call center",
                                   99: "Not Applicable",
                                   pd.NA: "Missing"},
            "mobile_recruit": {0: "Recruit survey was not completed on mobile device",
                               1: "Recruit survey was completed on mobile device",
                               pd.NA: "Missing"},
            "participate_future_studies": {1: "Yes",
                                           2: "No",
                                           pd.NA: "Missing"},
            "hh_iscomplete": {0: "No",
                              1: "Yes",
                              pd.NA: "Missing"},
            "language_pref": {1: "English",
                              2: "Spanish",
                              97: "Other",
                              pd.NA: "Missing"},
            "hhsize": {**{key: str(key) for key in list(range(0, 12))},
                       **{12: "12+",
                          pd.NA: "Missing"}},
            "numadults": {**{key: str(key) for key in list(range(0, 12))},
                          **{12: "12+",
                             pd.NA: "Missing"}},
            "numkids": {**{key: str(key) for key in list(range(0, 11))},
                        **{11: "11+",
                           pd.NA: "Missing"}},
            "numworkers": {**{key: str(key) for key in list(range(0, 11))},
                           **{11: "11+",
                              pd.NA: "Missing"}},
            "vehicle_count": {**{key: str(key) for key in list(range(0, 7))},
                              **{7: "7+",
                                 pd.NA: "Missing"}},
            "bicycle_count": {**{key: str(key) for key in list(range(0, 10))},
                              **{10: "10+",
                                 pd.NA: "Missing"}},
            "share_car": {0: "No",
                          1: "Yes",
                          pd.NA: "Missing"},
            "share_bike": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "share_vanpool": {0: "No",
                              1: "Yes",
                              pd.NA: "Missing"},
            "res_duration": {1: "Less than a year",
                             2: "Between 1 and 2 years",
                             3: "Between 2 and 3 years",
                             4: "Between 3 and 5 years",
                             5: "Between 5 and 10 years",
                             6: "Between 10 and 20 years",
                             7: "More than 20 years",
                             pd.NA: "Missing"},
            "rent_own": {1: "Own/Buying (paying mortgage)",
                         2: "Rent",
                         3: "Provided by job or military",
                         97: "Other",
                         99: "Prefer not to answer",
                         pd.NA: "Missing"},
            "res_type": {1: "Single-family house (detached house)",
                         2: "Townhouse (attached house)",
                         3: "Building with 3 or fewer apartments/condos",
                         4: "Building with 4 or more apartments/condos",
                         5: "Mobile home/trailer",
                         6: "Dorm, barracks, or institutional housing",
                         97: "Other (including boat, RV, van, etc.)",
                         pd.NA: "Missing"},
            "hhincome_detailed": {1: "Under $15,000",
                                  2: "$15,000-$29,999",
                                  3: "$30,000-$44,999",
                                  4: "$45,000-$59,999",
                                  5: "$60,000-$74,999",
                                  6: "$75,000-$99,999",
                                  7: "$100,000-$124,999",
                                  8: "$125,000-$149,999",
                                  9: "$150,000-$199,999",
                                  10: "$200,000-$249,999",
                                  11: "$250,000 or more",
                                  99: "Prefer not to answer",
                                  pd.NA: "Missing"},
            "hhincome_broad": {1: "Under $30,000",
                               2: "$30,000-$59,999",
                               3: "$60,000-$99,999",
                               4: "$100,000-$149,999",
                               5: "$150,000 or more",
                               99: "Prefer not to answer",
                               pd.NA: "Missing"},
            "tool_paper": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "tool_freq_paper": {1: "Less than once a week",
                                2: "A few times a week",
                                3: "Daily or more",
                                pd.NA: "Not Applicable"},
            "tool_carnav": {0: "No",
                            1: "Yes",
                            pd.NA: "Missing"},
            "tool_freq_carnav": {1: "Less than once a week",
                                 2: "A few times a week",
                                 3: "Daily or more",
                                 pd.NA: "Not Applicable"},
            "tool_511sd": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "tool_freq_511sd": {1: "Less than once a week",
                                2: "A few times a week",
                                3: "Daily or more",
                                pd.NA: "Not Applicable"},
            "tool_apple": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "tool_freq_apple": {1: "Less than once a week",
                                2: "A few times a week",
                                3: "Daily or more",
                                pd.NA: "Not Applicable"},
            "tool_car2go": {0: "No",
                            1: "Yes",
                            pd.NA: "Missing"},
            "tool_freq_car2go": {1: "Less than once a week",
                                 2: "A few times a week",
                                 3: "Daily or more",
                                 pd.NA: "Not Applicable"},
            "tool_google": {0: "No",
                            1: "Yes",
                            pd.NA: "Missing"},
            "tool_freq_google": {1: "Less than once a week",
                                 2: "A few times a week",
                                 3: "Daily or more",
                                 pd.NA: "Not Applicable"},
            "tool_icommutesd": {0: "No",
                                1: "Yes",
                                pd.NA: "Missing"},
            "tool_freq_icommutesd": {1: "Less than once a week",
                                     2: "A few times a week",
                                     3: "Daily or more",
                                     pd.NA: "Not Applicable"},
            "tool_lyft": {0: "No",
                          1: "Yes",
                          pd.NA: "Missing"},
            "tool_freq_lyft": {1: "Less than once a week",
                               2: "A few times a week",
                               3: "Daily or more",
                               pd.NA: "Not Applicable"},
            "tool_mapmy": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "tool_freq_mapmy": {1: "Less than once a week",
                                2: "A few times a week",
                                3: "Daily or more",
                                pd.NA: "Not Applicable"},
            "tool_mapquest": {0: "No",
                              1: "Yes",
                              pd.NA: "Missing"},
            "tool_freq_mapquest": {1: "Less than once a week",
                                   2: "A few times a week",
                                   3: "Daily or more",
                                   pd.NA: "Not Applicable"},
            "tool_sdmts": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "tool_freq_sdmts": {1: "Less than once a week",
                                2: "A few times a week",
                                3: "Daily or more",
                                pd.NA: "Not Applicable"},
            "tool_nctd": {0: "No",
                          1: "Yes",
                          pd.NA: "Missing"},
            "tool_freq_nctd": {1: "Less than once a week",
                               2: "A few times a week",
                               3: "Daily or more",
                               pd.NA: "Not Applicable"},
            "tool_waze": {0: "No",
                          1: "Yes",
                          pd.NA: "Missing"},
            "tool_freq_waze": {1: "Less than once a week",
                               2: "A few times a week",
                               3: "Daily or more",
                               pd.NA: "Not Applicable"},
            "tool_uber": {0: "No",
                          1: "Yes",
                          pd.NA: "Missing"},
            "tool_freq_uber": {1: "Less than once a week",
                               2: "A few times a week",
                               3: "Daily or more",
                               pd.NA: "Not Applicable"},
            "tool_other": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "tool_freq_other": {1: "Less than once a week",
                                2: "A few times a week",
                                3: "Daily or more",
                                pd.NA: "Not Applicable"},
            "tool_none": {0: "No",
                          1: "Yes",
                          pd.NA: "Missing"},
            "border_freq_1": {**{key: str(key) for key in list(range(0, 10))},
                              **{10: "10+",
                                 98: "Do not know",
                                 pd.NA: "Missing"}}
        }

        for field in mappings:
            # define using pd.Categorical to maintain defined category order
            # without setting ordered parameter to True
            df[field] = pd.Categorical(
                df[field].map(mappings[field]),
                categories=mappings[field].values()
            )

        # apply conditional recodes
        df.loc[df.hhgroup == "Group 3: Online diary only", "num_rmove_participants"] = "Not Applicable"
        df.loc[df.language_pref != "Other", "language_pref_other"] = "Not Applicable"
        df.loc[df.tool_other == "No", "tool_other_specify"] = "Not Applicable"

        # WKT household location point geometry from lat/long in EPSG:2230
        df["shape"] = self.point_wkt(zip(df["home_lng"], df["home_lat"]), "EPSG:2230")

        # rename columns
        df.rename(columns={
            "hhid": "household_id",
            "hhgroup": "sample_group",
            "traveldate_start": "travel_date_start",
            "callcenter_recruit": "recruit_survey_where",
            "mobile_recruit": "recruit_survey_mobile",
            "recruit_start_pt": "recruit_survey_start",
            "recruit_end_pt": "recruit_survey_end",
            "num_rmove_participants": "number_rmove_participants",
            "hh_iscomplete": "household_completed",
            "numdays_complete": "completed_days",
            "language_pref": "language",
            "language_pref_other": "language_other",
            "hhsize": "persons",
            "numadults": "adults",
            "numkids": "children",
            "numworkers": "workers",
            "vehicle_count": "vehicles",
            "bicycle_count": "bicycles",
            "share_car": "has_share_car",
            "share_bike": "has_share_bicycle",
            "share_vanpool": "has_share_vanpool",
            "home_address": "address",
            "home_lat": "latitude",
            'home_lng': "longitude",
            "res_duration": "residence_duration",
            "rent_own": "residence_tenure_status",
            "res_type": "residence_type",
            "hhincome_detailed": "income_category_detailed",
            "hhincome_broad": "income_category_broad",
            "tool_paper": "use_paper_maps",
            "tool_freq_paper": "freq_paper_maps",
            "tool_carnav": "use_car_navigation",
            "tool_freq_carnav": "freq_car_navigation",
            "tool_511sd": "use_511sd",
            "tool_freq_511sd": "freq_511sd",
            "tool_apple": "use_apple_maps",
            "tool_freq_apple": "freq_apple_maps",
            "tool_car2go": "use_car2go",
            "tool_freq_car2go": "freq_car2go",
            "tool_google": "use_google_maps",
            "tool_freq_google": "freq_google_maps",
            "tool_icommutesd": "use_icommutesd",
            "tool_freq_icommutesd": "freq_icommutesd",
            "tool_lyft": "use_lyft",
            "tool_freq_lyft": "freq_lyft",
            "tool_mapmy": "use_mapmyride",
            "tool_freq_mapmy": "freq_mapmyride",
            "tool_mapquest": "use_mapquest",
            "tool_freq_mapquest": "freq_mapquest",
            "tool_sdmts": "use_sdmts",
            "tool_freq_sdmts": "freq_sdmts",
            "tool_nctd": "use_nctd",
            "tool_freq_nctd": "freq_nctd",
            "tool_waze": "use_waze",
            "tool_freq_waze": "freq_waze",
            "tool_uber": "use_uber",
            "tool_freq_uber": "freq_uber",
            "tool_other": "use_other_tool",
            "tool_other_specify": "specify_other_tool",
            "tool_freq_other": "freq_other_tool",
            "tool_none": "use_no_navigation_tools",
            "border_freq_1": "freq_cross_border",
            "hh_init_wt": "weight_household_initial",
            "hh_weight_4x": "weight_household_4x",
            "hh_final_weight_456x": "weight_household_456x"
        },
            inplace=True)

        return df[["household_id",
                   "sample_segment",
                   "sample_group",
                   "travel_date_start",
                   "recruit_survey_where",
                   "recruit_survey_mobile",
                   "recruit_survey_start",
                   "recruit_survey_end",
                   "number_rmove_participants",
                   "participate_future_studies",
                   "household_completed",
                   "completed_days",
                   "language",
                   "language_other",
                   "persons",
                   "adults",
                   "children",
                   "workers",
                   "vehicles",
                   "bicycles",
                   "has_share_car",
                   "has_share_bicycle",
                   "has_share_vanpool",
                   "address",
                   "latitude",
                   "longitude",
                   "shape",
                   "residence_duration",
                   "residence_tenure_status",
                   "residence_type",
                   "income_category_detailed",
                   "income_category_broad",
                   "use_paper_maps",
                   "freq_paper_maps",
                   "use_car_navigation",
                   "freq_car_navigation",
                   "use_511sd",
                   "freq_511sd",
                   "use_apple_maps",
                   "freq_apple_maps",
                   "use_car2go",
                   "freq_car2go",
                   "use_google_maps",
                   "freq_google_maps",
                   "use_icommutesd",
                   "freq_icommutesd",
                   "use_lyft",
                   "freq_lyft",
                   "use_mapmyride",
                   "freq_mapmyride",
                   "use_mapquest",
                   "freq_mapquest",
                   "use_sdmts",
                   "freq_sdmts",
                   "use_nctd",
                   "freq_nctd",
                   "use_waze",
                   "freq_waze",
                   "use_uber",
                   "freq_uber",
                   "use_other_tool",
                   "specify_other_tool",
                   "freq_other_tool",
                   "use_no_navigation_tools",
                   "freq_cross_border",
                   "weight_household_initial",
                   "weight_household_4x",
                   "weight_household_456x"]]

    @property
    @lru_cache(maxsize=1)
    def intercept(self) -> pd.DataFrame:
        """ Active Transportation Intercept survey containing the following columns:
                household_id - unique identifier of household
                survey_status - survey completion status
                survey_start - survey start time (PST)
                survey_end - survey end time (PST)
                survey_date - survey_date (PST)
                pilot_study - survey administered during pilot study
                origin_purpose - primary purpose at trip origin
                employment_status - employment status
                student_status - student status
                origin_address - trip origin address
                origin_latitude - trip origin latitude
                origin_longitude - trip origin longitude
                destination_purpose - primary purpose at trip destination
                destination_address - trip destination address
                destination_latitude - trip destination latitude
                destination_longitude - trip destination longitude
                distance_beeline - o-d beeline distance (miles)
                distance_beeline_bin - o-d beeline distance category
                visit_work - employed or volunteer/intern; if trip o/d is not work,
                    will visit work before returning home
                visit_school - student; if trip o/d is not school, will visit
                    school before returning home
                number_household_vehicles - number of household vehicles
                number_children_0_15 - number of children age <=15 in household
                number_children_16_17 - number of children age 16-17 in household
                number_adults - number of adults 18+ in household
                age - respondent age
                smartphone - smartphone ownership
                resident - resident of San Diego County
                bike_party - number of members in bike party (not including self)
                bike_share - respondent or members of bike party using bike share
                gender - respondent gender
                intercept_site - intercept site/location
                intercept_direction - direction participant was traveling when intercepted
                language - language in which survey was conducted
                rmove_qualify - respondent qualified to participate in
                    part two (rMove diary)
                opt_out - opted to not participate in part two (rMove diary)
                rmove_participate - respondent qualified and agreed to participate
                    in part two (rMove diary)
                rmove_complete - respondent completed part two (rMove diary),
                    90% of trip surveys complete and 5+ daily surveys complete
                recruit_complete - respondent completed the recruit survey
                survey_time_peak - survey was completed during peak hours
                expansion_site - site/location used to determine expansion factor
                expansion_factor - final expansion factor """

        df = pd.read_csv(
            "../data/at/SDRTS_AT_Intercept_Data_20170608.csv",
            usecols=["hhid",
                     "status",
                     "survey_start_pdt",
                     "survey_end_pdt",
                     "survey_day",
                     "pilot_study",
                     "origin_purpose_1_1",
                     "employment_int_1_1",
                     "student_int_1_1",
                     "origin_loc_address_1",
                     "origin_loc_lat_1",
                     "origin_loc_lng_1",
                     "dest_purpose_1_1",
                     "dest_loc_address_1",
                     "dest_loc_lat_1",
                     "dest_loc_lng_1",
                     "distance_beeline",
                     "distance_beeline_agg",
                     "work_int_1_1",
                     "work_int_1_2",
                     "school_int_1_1",
                     "school_int_1_2",
                     "vehicle_count_1_1",
                     "age_groups_1_1",
                     "age_groups_1_2",
                     "age_groups_1_3",
                     "age_int_1_1",
                     "smartphone_int_1_1",
                     "resident_int_1_1",
                     "bikeparty_1_1",
                     "bikeshare_1_1",
                     "gender_1_1",
                     "intercept_location_1_1",
                     "intercept_location_dir_1_1",
                     "language_1_1",
                     "rmove_qualify",
                     "optout_1_1",
                     "rmove_participate",
                     "rmove_complete",
                     "recruit_complete",
                     "survey_time_peak",
                     "expansion_site",
                     "exp_factor"],
            dtype={"status": "Int8",
                   "pilot_study": "Int8",
                   "origin_purpose_1_1": "Int8",
                   "employment_int_1_1": "Int8",
                   "student_int_1_1": "Int8",
                   "dest_purpose_1_1": "Int8",
                   "distance_beeline_agg": "Int8",
                   "work_int_1_1": "Int8",
                   "work_int_1_2": "Int8",
                   "school_int_1_1": "Int8",
                   "school_int_1_2": "Int8",
                   "vehicle_count_1_1": "Int8",
                   "age_int_1_1": "Int8",
                   "smartphone_int_1_1": "Int8",
                   "resident_int_1_1": "Int8",
                   "bikeparty_1_1": "Int8",
                   "bikeshare_1_1": "Int8",
                   "gender_1_1": "Int8",
                   "intercept_location_1_1": "Int8",
                   "intercept_location_dir_1_1": "Int8",
                   "language_1_1": "Int8",
                   "rmove_qualify": "Int8",
                   "optout_1_1": "Int8",
                   "rmove_participate": "Int8",
                   "rmove_complete": "Int8",
                   "recruit_complete": "Int8",
                   "survey_time_peak": "Int8",
                   "expansion_site": "Int8"},
            na_values=["", " "]
        )

        # apply exhaustive field mappings where applicable
        mappings = {
            "status": {0: "Incomplete",
                       1: "Complete",
                       pd.NA: "Missing"},
            "pilot_study": {0: "No",
                            1: "Yes",
                            pd.NA: "Missing"},
            "origin_purpose_1_1": {1: "My home",
                                   2: "My work or a work-related place",
                                   3: "My school",
                                   4: "Someone elses home",
                                   5: "A business (e.g., shopping, errand, banking, doctor, etc.)",
                                   6: "A restaurant",
                                   7: "A leisure activity (e.g., museum, gym, sporting event, etc.)",
                                   8: "Pickup or drop-off child at daycare, school, etc.",
                                   9: "Other",
                                   pd.NA: "Missing"},
            "employment_int_1_1": {1: "Employed full-time (paid) 35+ hours/week",
                                   2: "Employed part-time (paid) up to 35 hours/week",
                                   3: "Unpaid volunteer or intern",
                                   4: "Not currently employed",
                                   pd.NA: "Missing"},
            "student_int_1_1": {1: "Not a student",
                                2: "Enrolled in higher education",
                                3: "Enrolled in high school (grades 9-12)",
                                4: "Enrolled in grade school or middle school (grades K-8)",
                                pd.NA: "Missing"},
            "dest_purpose_1_1": {1: "My home",
                                 2: "My work or a work-related place",
                                 3: "My school",
                                 4: "Someone elses home",
                                 5: "A business (e.g., shopping, errand, banking, doctor, etc.)",
                                 6: "A restaurant",
                                 7: "A leisure activity (e.g., museum, gym, sporting event, etc.)",
                                 8: "Pickup or drop-off child at daycare, school, etc.",
                                 9: "Other",
                                 pd.NA: "Missing"},
            "distance_beeline_agg": {0: "0-2 miles",
                                     2: "2-4 miles",
                                     4: "4-6 miles",
                                     6: "6-8 miles",
                                     8: "8-10 miles",
                                     10: "10-12 miles",
                                     12: "12-14 miles",
                                     14: "14-16 miles",
                                     16: "16-18 miles",
                                     18: "18-20 miles",
                                     20: "20 miles or more",
                                     pd.NA: "Missing"},
            "work_int_1_1": {1: "Yes",
                             2: "No",
                             3: "Not Applicable",
                             pd.NA: "Missing"},
            "work_int_1_2": {1: "Yes",
                             2: "No",
                             3: "Not Applicable",
                             pd.NA: "Missing"},
            "school_int_1_1": {1: "Yes",
                               2: "No",
                               3: "Not Applicable",
                               pd.NA: "Missing"},
            "school_int_1_2": {1: "Yes",
                               2: "No",
                               3: "Not Applicable",
                               pd.NA: "Missing"},
            "vehicle_count_1_1": {0: 0,
                                  1: 1,
                                  2: 2,
                                  3: 3,
                                  4: 4,
                                  5: 5,
                                  6: 6,
                                  7: "7+",
                                  pd.NA: "Missing"},
            "age_int_1_1": {1: "16-17 years",
                            2: "18-24 years",
                            3: "25-34 years",
                            4: "35-44 years",
                            5: "45-49 years",
                            6: "50-54 years",
                            7: "55-59 years",
                            8: "60-64 years",
                            9: "65 years or older",
                            pd.NA: "Missing"},
            "smartphone_int_1_1": {1: "Yes, an Android smartphone",
                                   2: "Yes, an Apple smartphone",
                                   3: "Yes, another type of smartphone",
                                   4: "No, I do not own a smartphone",
                                   pd.NA: "Missing"},
            "resident_int_1_1": {1: "Yes",
                                 2: "No",
                                 pd.NA: "Missing"},
            "bikeparty_1_1": {1: 0,
                              2: 1,
                              3: 2,
                              4: 3,
                              5: 4,
                              6: "5+",
                              pd.NA: "Missing"},
            "bikeshare_1_1": {1: "Yes",
                              2: "No",
                              pd.NA: "Missing"},
            "gender_1_1": {1: "Male",
                           2: "Female",
                           3: "Do not know",
                           pd.NA: "Missing"},
            "intercept_location_1_1": {
                1: "1 - (Oceanside) - S. Pacific Street and Tyson St",
                2: "2 - (Oceanside) - Rail Trail and Elm Street",
                3: "3 - (Oceanside) - N Coast Highway and Topeka St (8/16/16 - 2/05/17)",
                4: "4 - (San Marcos) - Valpreda Rd and Mission Rd",
                5: "5 - (San Marcos) - Campus Way and Campus View Dr",
                6: "6 - (Escondido) - Rock Springs Rd and W Mission Ave",
                7: "7 - (Escondido) - Centre City Pkwy and W Valley Pkwy",
                8: "8 - (Solana Beach) - Coast Highway and Lomas Santa Fe/Plaza St",
                9: "9 - (Chula Vista) - Broadway, Between E Street and Flower St",
                10: "10 - (San Diego - La Jolla) - N Torrey Pines Rd and La Jolla Shores Dr",
                11: "11 - (San Diego - La Jolla) - Gilman Dr between Osler Ln and La Jolla Village Dr",
                12: "12 - (San Diego - Pacific Beach) - Fanuel St and Crown Point Bike Path",
                13: "13 - (San Diego - Pacific Beach) - Ingraham St and Hornblend St",
                14: "14 - (San Diego - Downtown) - Union St and Ash St",
                15: "15 - (San Diego - Downtown) - 4th Ave and C St",
                16: "16 - (San Diego - Downtown) - 5th Ave and Broadway (8/16/16 - 02/05/17)",
                17: "17 - (San Diego - Downtown) - 10th Ave and Market St",
                18: "18 - (San Diego - North Park) - Park Blvd and Myrtle St",
                19: "19 - (San Diego - North Park) - 30th St and University Ave",
                20: "20 - (San Diego - North Park) - Oregon St and El Cajon Blvd",
                21: "21 - (San Diego - North Park) - Utah St and Meade Ave",
                22: "22 - (San Diego - College Area) - Collwood Blvd and Montezuma Rd",
                23: "23 - (San Diego - College Area) - E Campus Dr and Montezuma Rd",
                24: "24 - (El Cajon) - Orlando St and Main St",
                25: "25 - (El Cajon) - Jamacha Road and E. Washington Ave",
                26: "26 - Other",
                27: "3 - (Oceanside) - N Coast Highway and Mission Ave (2/06/17 - 2/16/17)",
                28: "16 - (San Diego - Downtown) - 5th Ave, between Elm St and Fir St (2/06/17 - 2/16/17)",
                pd.NA: "Missing"
            },
            "intercept_location_dir_1_1": {1: "North",
                                           2: "East",
                                           3: "South",
                                           4: "West",
                                           pd.NA: "Missing"},
            "language_1_1": {1: "English",
                             2: "Spanish",
                             3: "Other",
                             pd.NA: "Missing"},
            "rmove_qualify": {0: "No",
                              1: "Yes",
                              pd.NA: "Missing"},
            "optout_1_1": {0: "No",
                           1: "Yes",
                           pd.NA: "Not Applicable"},
            "rmove_participate": {0: "No",
                                  1: "Yes",
                                  pd.NA: "Missing"},
            "rmove_complete": {0: "No",
                               1: "Yes",
                               pd.NA: "Not Applicable"},
            "recruit_complete": {0: "No (dropped out)",
                                 1: "Yes",
                                 pd.NA: "Not Applicable"},
            "survey_time_peak": {0: "No",
                                 1: "Yes",
                                 pd.NA: "Missing"},
            "expansion_site": {
                1: "1 - (Oceanside) - S. Pacific Street and Tyson St",
                2: "2 - (Oceanside) - Rail Trail and Elm Street",
                3: "3 - (Oceanside) - N Coast Highway and Topeka St (8/16/16 - 2/05/17)",
                4: "4 - (San Marcos) - Valpreda Rd and Mission Rd",
                5: "5 - (San Marcos) - Campus Way and Campus View Dr",
                6: "6 - (Escondido) - Rock Springs Rd and W Mission Ave",
                7: "7 - (Escondido) - Centre City Pkwy and W Valley Pkwy",
                8: "8 - (Solana Beach) - Coast Highway and Lomas Santa Fe/Plaza St",
                9: "9 - (Chula Vista) - Broadway, Between E Street and Flower St",
                10: "10 - (San Diego - La Jolla) - N Torrey Pines Rd and La Jolla Shores Dr",
                11: "11 - (San Diego - La Jolla) - Gilman Dr between Osler Ln and La Jolla Village Dr",
                12: "12 - (San Diego - Pacific Beach) - Fanuel St and Crown Point Bike Path",
                13: "13 - (San Diego - Pacific Beach) - Ingraham St and Hornblend St",
                14: "14 - (San Diego - Downtown) - Union St and Ash St",
                15: "15 - (San Diego - Downtown) - 4th Ave and C St",
                16: "16 - (San Diego - Downtown) - 5th Ave and Broadway (8/16/16 - 02/05/17)",
                17: "17 - (San Diego - Downtown) - 10th Ave and Market St",
                18: "18 - (San Diego - North Park) - Park Blvd and Myrtle St",
                19: "19 - (San Diego - North Park) - 30th St and University Ave",
                20: "20 - (San Diego - North Park) - Oregon St and El Cajon Blvd",
                21: "21 - (San Diego - North Park) - Utah St and Meade Ave",
                22: "22 - (San Diego - College Area) - Collwood Blvd and Montezuma Rd",
                23: "23 - (San Diego - College Area) - E Campus Dr and Montezuma Rd",
                24: "24 - (El Cajon) - Orlando St and Main St",
                25: "25 - (El Cajon) - Jamacha Road and E. Washington Ave",
                26: "26 - Other",
                27: "3 - (Oceanside) - N Coast Highway and Mission Ave (2/06/17 - 2/16/17)",
                28: "16 - (San Diego - Downtown) - 5th Ave, between Elm St and Fir St (2/06/17 - 2/16/17)",
                pd.NA: "Missing"
            }
        }

        for field in mappings:
            # define using pd.Categorical to maintain defined category order
            # without setting ordered parameter to True
            df[field] = pd.Categorical(
                df[field].map(mappings[field]),
                categories=mappings[field].values()
            )

        # combine work visit variables
        df["visit_work"] = np.where(
            df.work_int_1_1.isin(["Missing", "Not Applicable"]),
            df.work_int_1_2,
            df.work_int_1_1
        )

        # combine school visit variables
        df["visit_school"] = np.where(
            df.school_int_1_1.isin(["Missing", "Not Applicable"]),
            df.school_int_1_2,
            df.school_int_1_1
        )

        # rename columns
        df.rename(columns={
            "hhid": "household_id",
            "status": "survey_status",
            "survey_start_pdt": "survey_start",
            "survey_end_pdt": "survey_end",
            "survey_day": "survey_date",
            "origin_purpose_1_1": "origin_purpose",
            "employment_int_1_1": "employment_status",
            "student_int_1_1": "student_status",
            "origin_loc_address_1": "origin_address",
            "origin_loc_lat_1": "origin_latitude",
            "origin_loc_lng_1": "origin_longitude",
            "dest_purpose_1_1": "destination_purpose",
            "dest_loc_address_1": "destination_address",
            "dest_loc_lat_1": "destination_latitude",
            "dest_loc_lng_1": "destination_longitude",
            "distance_beeline": "distance_beeline",
            "distance_beeline_agg": "distance_beeline_bin",
            "vehicle_count_1_1": "number_household_vehicles",
            "age_groups_1_1": "number_children_0_15",
            "age_groups_1_2": "number_children_16_17",
            "age_groups_1_3": "number_adults",
            "age_int_1_1": "age",
            "smartphone_int_1_1": "smartphone",
            "resident_int_1_1": "resident",
            "bikeparty_1_1": "bike_party",
            "bikeshare_1_1": "bike_share",
            "gender_1_1": "gender",
            "intercept_location_1_1": "intercept_site",
            "intercept_location_dir_1_1": "intercept_direction",
            "language_1_1": "language",
            "rmove_qualify": "rmove_qualify",
            "optout_1_1": "opt_out",
            "rmove_participate": "rmove_participate",
            "rmove_complete": "rmove_complete",
            "recruit_complete": "recruit_complete",
            "survey_time_peak": "survey_time_peak",
            "expansion_site": "expansion_site",
            "exp_factor": "expansion_factor"
        },
            inplace=True)

        return df[["household_id",
                   "survey_status",
                   "survey_start",
                   "survey_end",
                   "survey_date",
                   "pilot_study",
                   "origin_purpose",
                   "employment_status",
                   "student_status",
                   "origin_address",
                   "origin_latitude",
                   "origin_longitude",
                   "destination_purpose",
                   "destination_address",
                   "destination_latitude",
                   "destination_longitude",
                   "distance_beeline",
                   "distance_beeline_bin",
                   "visit_work",
                   "visit_school",
                   "number_household_vehicles",
                   "number_children_0_15",
                   "number_children_16_17",
                   "number_adults",
                   "age",
                   "smartphone",
                   "resident",
                   "bike_party",
                   "bike_share",
                   "gender",
                   "intercept_site",
                   "intercept_direction",
                   "language",
                   "rmove_qualify",
                   "opt_out",
                   "rmove_participate",
                   "rmove_complete",
                   "recruit_complete",
                   "survey_time_peak",
                   "expansion_site",
                   "expansion_factor"]]

    @property
    @lru_cache(maxsize=1)
    def location(self) -> dict:
        """ Location tracking data for rMoves trips consisting of two
        data-sets returned in a Dictionary as Pandas DataFrames
            1). points: ordered location point data
                    point_id - surrogate key ordered by (trip_id_location, collected_at)
                    trip_id_location - trip identifier for joining to trip list
                    collected_at - datetime of location data collected
                    accuracy - location accuracy in meters
                    heading - heading in degrees
                    speed - speed in meters per second
                    latitude - location point latitude
                    longitude - location point longitude
                    shape - location point geometry (EPSG: 2230)
            2). lines: trip paths built from ordered location point data
                    trip_id_location - trip identifier for joining to trips table
                    shape - trip path linestring geometry (EPSG:2230) """

        # combine HHTBS and AT data-sets
        points = pd.concat(
            [
                pd.read_csv(
                    "../data/sdrts/SDRTS_Location_Data_20170731.csv",
                    usecols=["tripid",
                             "collected_at",
                             "accuracy",
                             "heading",
                             "speed",
                             "lat",
                             "lng"]
                ),
                pd.read_csv(
                    "../data/at/SDRTS_AT_Location_Data_20170809.csv",
                    usecols=["tripid",
                             "collected_at",
                             "accuracy",
                             "heading",
                             "speed",
                             "lat",
                             "lng"]
                )
            ],
            ignore_index=True
        )

        # for each tripid, return the ordered coordinates
        # by collection time as (longitude, latitude) tuples in a list
        # then create the linestring path from the ordered coordinate tuples
        points.sort_values(by=["tripid", "collected_at"], inplace=True)
        points["coordinates"] = list(zip(points.lng, points.lat))
        lines = points.groupby("tripid")["coordinates"].apply(list).reset_index(name="coordinates")
        lines["shape"] = self.line_wkt(lines["coordinates"], "EPSG:2230")

        # WKT location point geometry from lat/long in EPSG:2230
        points["shape"] = self.point_wkt(zip(points["lng"], points["lat"]), "EPSG:2230")

        # add surrogate key to points DataFrame
        points["point_id"] = np.arange(len(points))

        # rename columns
        points.rename(columns={
            "tripid": "trip_id_location",
            "lat": "latitude",
            "lng": "longitude"
        },
            inplace=True)

        lines.rename(columns={
            "tripid": "trip_id_location"
        },
            inplace=True)

        return {
            "points": points[["point_id",
                              "trip_id_location",
                              "collected_at",
                              "accuracy",
                              "heading",
                              "speed",
                              "latitude",
                              "longitude",
                              "shape"]],
            "lines": lines[["trip_id_location",
                            "shape"]]
        }

    @property
    @lru_cache(maxsize=1)
    def persons(self) -> pd.DataFrame:
        """ Person list containing the following columns:
                person_id - unique identifier of person
                household_id - unique identifier of household
                person_number - unique identifier of person within household
                travel_date_start - assigned travel date (online or rMove)
                rmove_participant - used rMove for Part 2
                relationship - relationship to primary respondent
                gender - gender
                age_category - age category
                employment_status - age 16+ employment status
                number_of_jobs - number of jobs
                adult_student_status - age 18+ student status
                educational_attainment - age 18+ educational attainment
                drivers_license - age 16+ has valid drivers license
                military_status - employed/volunteer military affiliation
                ethnicity_americanindian_alaskanative - age 16+ ethnicity american indian/alaska native
                ethnicity_asian - age 16+ ethnicity asian
                ethnicity_black - age 16+ ethnicity black/african american
                ethnicity_hispanic - age 16+ ethnicity hispanic/latino
                ethnicity_hawaiian_pacific - age 16+ ethnicity native hawaiian/pacific islander
                ethnicity_white - age 16+ ethnicity white
                ethnicity_other - age 16+ ethnicity other
                disability - age 16+ disability or illness that affects ability to travel
                height - age 18+ height in inches
                weight - age 18+ weight in pounds
                physical_activity - age 18+ how physically active are you in a typical week
                transit_frequency - age 16+ use transit how often
                transit_pass - uses transit 1+ days/week, uses a transit pass
                school_type - student (child or adult) type of school attended
                school_frequency - student (adult or child, not homeschooled) how often travel to school
                other_school - student (adult or child, not homeschooled) travel to secondary/other school
                school_mode - travels to school, typical travel mode
                daycare_open - attends daycare, time daycare opens
                daycare_close - attends daycare, time daycare closes
                work_location_type - employed/volunteer (non-active duty or deployed military) workplace location type
                occupation - employed/volunteer (non-active duty or deployed military) occupation
                industry - employed/volunteer (non-active duty or deployed military) industry
                hours_worked - employed/volunteer (non-active duty or deployed military) number of hours worked per week
                commute_frequency - workplace is fixed/varied, typical commute frequency
                commute_mode - commute frequency <> never, typical commute mode
                work_arrival_frequency - commute frequency <> never, arrival time flexibility
                work_parking_payment - commute mode is vehicle, work parking payment method
                work_parking_cost - pays some/all work parking costs, monthly cost ($)
                work_parking_cost_dk - pays some/all work parking costs, do not know cost
                work_parking_ease - commute mode is vehicle, how easy to find parking spot at work
                telecommute_frequency - if job type is not work at home only, typical telecommute frequency
                commute_subsidy_none - commute, no commute subsidy
                commute_subsidy_parking - commute, free/subsidized parking
                commute_subsidy_transit - commute, free/subsidized transit fare
                commute_subsidy_vanpool - commute, free/subsidized vanpool
                commute_subsidy_cash - commute, cash incentives for carpooling/walking/biking
                commute_subsidy_other - commute, other commute subsidy
                commute_subsidy_specify - commute, specify other commute subsidy
                has_second_home - person has a second or part-time home
                second_home_address - secondary home address
                second_home_latitude - secondary home latitude
                second_home_longitude - secondary home longitude
                second_home_shape - secondary home WKT geometry (EPSG:2230)
                school_address - primary school address
                school_latitude - primary school latitude
                school_longitude - primary school longitude
                school_shape - primary school WKT geometry (EPSG: 2230)
                second_school_address - secondary school address
                second_school_latitude - secondary school latitude
                second_school_longitude - secondary school longitude
                second_school_shape - secondary school WKT geometry (EPSG: 2230)
                work_address - primary work address
                work_latitude - primary work latitude
                work_longitude - primary work longitude
                work_shape - primary work WKT geometry (EPSG:2230)
                second_work_address - secondary work address
                second_work_latitude - secondary work latitude
                second_work_longitude - secondary work longitude
                second_work_shape - secondary work WKT geometry (EPSG:2230)
                smartphone_type - age 16+ smartphone type owned
                smartphone_age - qualified smartphone obtained in past four years
                smartphone_child - age 16-17 with qualified smartphone, allowed to use rMove
                diary_callcenter - travel diary completed by call center
                diary_mobile - travel diary completed on mobile device
                rmove_activated - rMove users when activated rMove/when proxy activated rMove
                completed_days - number of days participant completed all surveys
                completed_day1 - participant completed all surveys on day 1
                completed_day2 - participant completed all surveys on day 2
                completed_day3 - participant completed all surveys on day 3
                completed_day4 - participant completed all surveys on day 4
                completed_day5 - participant completed all surveys on day 5
                completed_day6 - participant completed all surveys on day 6
                completed_day7 - participant completed all surveys on day 7 """

        # load AT data-set
        at_df = pd.read_csv(
            "../data/at/SDRTS_AT_Person_Data_20170831.csv",
            usecols=["personid",
                     "hhid",
                     "pernum",
                     "traveldate_start",
                     "rmove_participant",
                     "relationship",
                     "gender",
                     "age",
                     "employment",
                     "jobs_count",
                     "student",
                     "education",
                     "license",
                     "military",
                     "ethnicity_amindian_alaska",
                     "ethnicity_asian",
                     "ethnicity_black",
                     "ethnicity_hispanic",
                     "ethnicity_hawaiian_pacific",
                     "ethnicity_white",
                     "ethnicity_other",
                     "ethnicity_prefernot",
                     "disability",
                     "height",
                     "weight_lbs",
                     "physical_activity",
                     "transit_freq",
                     "transitpass",
                     "schooltype",
                     "school_freq",
                     "other_school",
                     "school_mode",
                     "daycare_early",
                     "daycare_late",
                     "job_type",
                     "occupation",
                     "industry",
                     "hours_work",
                     "commute_freq",
                     "commute_mode",
                     "work_flex",
                     "work_park_pay",
                     "work_park_cost",
                     "work_park_cost_dk",
                     "work_park_ease",
                     "telecommute_freq",
                     "commute_subsidy_none",
                     "commute_subsidy_parking",
                     "commute_subsidy_transit",
                     "commute_subsidy_vanpool",
                     "commute_subsidy_cash",
                     "commute_subsidy_other",
                     "commute_subsidy_specify",
                     "secondhome",
                     "secondhome_address",
                     "secondhome_lat",
                     "secondhome_lng",
                     "mainschool_address",
                     "mainschool_lat",
                     "mainschool_lng",
                     "secondschool_address",
                     "secondschool_lat",
                     "secondschool_lng",
                     "work_address",
                     "work_lat",
                     "work_lng",
                     "secondwork_address",
                     "secondwork_lat",
                     "secondwork_lng",
                     # "smartphone_type",
                     # "smartphone_age",
                     # "child_smartphone",
                     # "callcenter_diary",
                     # "mobile_diary",
                     # "activated_rmove",
                     "numdays_complete",
                     "day1_complete",
                     "day2_complete",
                     "day3_complete",
                     "day4_complete",
                     "day5_complete",
                     "day6_complete",
                     "day7_complete"],
            dtype={"rmove_participant": "Int8",
                   "relationship": "Int8",
                   "gender": "Int8",
                   "age": "Int8",
                   "employment": "Int8",
                   "jobs_count": "Int8",
                   "student": "Int8",
                   "education": "Int8",
                   "license": "Int8",
                   "military": "Int8",
                   "ethnicity_amindian_alaska": "Int8",
                   "ethnicity_asian": "Int8",
                   "ethnicity_black": "Int8",
                   "ethnicity_hispanic": "Int8",
                   "ethnicity_hawaiian_pacific": "Int8",
                   "ethnicity_white": "Int8",
                   "ethnicity_other": "Int8",
                   "ethnicity_prefernot": "Int8",
                   "disability": "Int8",
                   "physical_activity": "Int8",
                   "transit_freq": "Int8",
                   "transitpass": "Int8",
                   "schooltype": "Int8",
                   "school_freq": "Int8",
                   "other_school": "Int8",
                   "school_mode": "Int8",
                   "daycare_early": "Int8",
                   "daycare_late": "Int8",
                   "job_type": "Int8",
                   "occupation": "Int8",
                   "industry": "Int8",
                   "hours_work": "Int8",
                   "commute_freq": "Int8",
                   "commute_mode": "Int8",
                   "work_flex": "Int8",
                   "work_park_pay": "Int8",
                   "work_park_cost_dk": "Int8",
                   "work_park_ease": "Int8",
                   "telecommute_freq": "Int8",
                   "commute_subsidy_none": "Int8",
                   "commute_subsidy_parking": "Int8",
                   "commute_subsidy_transit": "Int8",
                   "commute_subsidy_vanpool": "Int8",
                   "commute_subsidy_cash": "Int8",
                   "commute_subsidy_other": "Int8",
                   "secondhome": "Int8",
                   # "smartphone_type": "Int8",
                   # "smartphone_age": "Int8",
                   # "child_smartphone": "Int8",
                   # "callcenter_diary": "Int8",
                   # "mobile_diary": "Int8",
                   "day1_complete": "Int8",
                   "day2_complete": "Int8",
                   "day3_complete": "Int8",
                   "day4_complete": "Int8",
                   "day5_complete": "Int8",
                   "day6_complete": "Int8",
                   "day7_complete": "Int8"}
        )

        # set values for variables not in AT data-set
        at_df["smartphone_type"] = 99  # new hardcoded Not Applicable value
        at_df["smartphone_age"] = 99  # new hardcoded Not Applicable value
        at_df["child_smartphone"] = 99  # new hardcoded Not Applicable value
        at_df["callcenter_diary"] = 99  # new hardcoded Not Applicable value
        at_df["mobile_diary"] = 99  # new hardcoded Not Applicable value

        # combine HHTBS and AT data-sets
        df = pd.concat(
            [
                pd.read_csv(
                    "../data/sdrts/SDRTS_Person_Data_20170731.csv",
                    usecols=["personid",
                             "hhid",
                             "pernum",
                             "traveldate_start",
                             "rmove_participant",
                             "relationship",
                             "gender",
                             "age",
                             "employment",
                             "jobs_count",
                             "student",
                             "education",
                             "license",
                             "military",
                             "ethnicity_amindian_alaska",
                             "ethnicity_asian",
                             "ethnicity_black",
                             "ethnicity_hispanic",
                             "ethnicity_hawaiian_pacific",
                             "ethnicity_white",
                             "ethnicity_other",
                             "ethnicity_prefernot",
                             "disability",
                             "height",
                             "weight_lbs",
                             "physical_activity",
                             "transit_freq",
                             "transitpass",
                             "schooltype",
                             "school_freq",
                             "other_school",
                             "school_mode",
                             "daycare_early",
                             "daycare_late",
                             "job_type",
                             "occupation",
                             "industry",
                             "hours_work",
                             "commute_freq",
                             "commute_mode",
                             "work_flex",
                             "work_park_pay",
                             "work_park_cost",
                             "work_park_cost_dk",
                             "work_park_ease",
                             "telecommute_freq",
                             "commute_subsidy_none",
                             "commute_subsidy_parking",
                             "commute_subsidy_transit",
                             "commute_subsidy_vanpool",
                             "commute_subsidy_cash",
                             "commute_subsidy_other",
                             "commute_subsidy_specify",
                             "secondhome",
                             "secondhome_address",
                             "secondhome_lat",
                             "secondhome_lng",
                             "mainschool_address",
                             "mainschool_lat",
                             "mainschool_lng",
                             "secondschool_address",
                             "secondschool_lat",
                             "secondschool_lng",
                             "work_address",
                             "work_lat",
                             "work_lng",
                             "secondwork_address",
                             "secondwork_lat",
                             "secondwork_lng",
                             "smartphone_type",
                             "smartphone_age",
                             "child_smartphone",
                             "callcenter_diary",
                             "mobile_diary",
                             "activated_rmove",
                             "numdays_complete",
                             "day1_complete",
                             "day2_complete",
                             "day3_complete",
                             "day4_complete",
                             "day5_complete",
                             "day6_complete",
                             "day7_complete"],
                    dtype={"rmove_participant": "Int8",
                           "relationship": "Int8",
                           "gender": "Int8",
                           "age": "Int8",
                           "employment": "Int8",
                           "jobs_count": "Int8",
                           "student": "Int8",
                           "education": "Int8",
                           "license": "Int8",
                           "military": "Int8",
                           "ethnicity_amindian_alaska": "Int8",
                           "ethnicity_asian": "Int8",
                           "ethnicity_black": "Int8",
                           "ethnicity_hispanic": "Int8",
                           "ethnicity_hawaiian_pacific": "Int8",
                           "ethnicity_white": "Int8",
                           "ethnicity_other": "Int8",
                           "ethnicity_prefernot": "Int8",
                           "disability": "Int8",
                           "physical_activity": "Int8",
                           "transit_freq": "Int8",
                           "transitpass": "Int8",
                           "schooltype": "Int8",
                           "school_freq": "Int8",
                           "other_school": "Int8",
                           "school_mode": "Int8",
                           "daycare_early": "Int8",
                           "daycare_late": "Int8",
                           "job_type": "Int8",
                           "occupation": "Int8",
                           "industry": "Int8",
                           "hours_work": "Int8",
                           "commute_freq": "Int8",
                           "commute_mode": "Int8",
                           "work_flex": "Int8",
                           "work_park_pay": "Int8",
                           "work_park_cost_dk": "Int8",
                           "work_park_ease": "Int8",
                           "telecommute_freq": "Int8",
                           "commute_subsidy_none": "Int8",
                           "commute_subsidy_parking": "Int8",
                           "commute_subsidy_transit": "Int8",
                           "commute_subsidy_vanpool": "Int8",
                           "commute_subsidy_cash": "Int8",
                           "commute_subsidy_other": "Int8",
                           "secondhome": "Int8",
                           "smartphone_type": "Int8",
                           "smartphone_age": "Int8",
                           "child_smartphone": "Int8",
                           "callcenter_diary": "Int8",
                           "mobile_diary": "Int8",
                           "day1_complete": "Int8",
                           "day2_complete": "Int8",
                           "day3_complete": "Int8",
                           "day4_complete": "Int8",
                           "day5_complete": "Int8",
                           "day6_complete": "Int8",
                           "day7_complete": "Int8"}
                ),
                at_df
            ],
            ignore_index=True
        )

        # apply exhaustive field mappings where applicable
        mappings = {
            "rmove_participant": {0: "No",
                                  1: "Yes",
                                  pd.NA: "Missing"},
            "relationship": {0: "Self",
                             1: "Husband/Wife/Partner",
                             2: "Son/Daughter/In-law",
                             3: "Mother/Father/In-law",
                             4: "Brother/Sister/In-law",
                             5: "Other relative",
                             6: "Roommate/Friend",
                             7: "Household help",
                             97: "Other",
                             pd.NA: "Missing"},
            "gender": {1: "Male",
                       2: "Female",
                       pd.NA: "Missing"},
            "age": {1: "Under 5 years old",
                    2: "5-15 years",
                    3: "16-17 years",
                    4: "18-24 years",
                    5: "25-34 years",
                    6: "35-44 years",
                    7: "45-49 years",
                    8: "50-54 years",
                    9: "55-59 years",
                    10: "60-64 years",
                    11: "65-74 years",
                    12: "75-79 years",
                    13: "80-84 years",
                    14: "85 years or older",
                    pd.NA: "Missing"},
            "employment": {1: "Employed full-time (paid) 35+ hours/week",
                           2: "Employed part-time (paid) up to 35 hours/week",
                           3: "Unpaid volunteer or intern",
                           4: "Not currently employed",
                           pd.NA: "Missing"},
            "jobs_count": {0: "0 (age 16+)",
                           1: "1",
                           2: "2",
                           3: "3",
                           4: "4",
                           5: "5+",
                           pd.NA: "Missing"},
            "student": {1: "Not a student",
                        2: "Part-time student",
                        3: "Full-time student",
                        pd.NA: "Missing"},
            "education": {1: "Less than high school",
                          2: "High school graduate/GED",
                          3: "Some college",
                          4: "Vocational/technical training",
                          5: "Associates degree",
                          6: "Bachelor degree",
                          7: "Graduate/post-graduate degree",
                          pd.NA: "Missing"},
            "license": {1: "Yes",
                        2: "No",
                        pd.NA: "Missing"},
            "military": {1: "No current affiliation with the military",
                         2: "Active duty within the San Diego region",
                         3: "Active duty outside of the San Diego region",
                         4: "Reserve or National Guard",
                         5: "Department of Defense civilian workforce and/or contractor",
                         6: "Veteran",
                         97: "Other affiliation (e.g., spouse or parent of active military)",
                         pd.NA: "Missing"},
            "ethnicity_amindian_alaska": {0: "No",
                                          1: "Yes",
                                          pd.NA: "Missing"},
            "ethnicity_asian": {0: "No",
                                1: "Yes",
                                pd.NA: "Missing"},
            "ethnicity_black": {0: "No",
                                1: "Yes",
                                pd.NA: "Missing"},
            "ethnicity_hispanic": {0: "No",
                                   1: "Yes",
                                   pd.NA: "Missing"},
            "ethnicity_hawaiian_pacific": {0: "No",
                                           1: "Yes",
                                           pd.NA: "Missing"},
            "ethnicity_white": {0: "No",
                                1: "Yes",
                                pd.NA: "Missing"},
            "ethnicity_other": {0: "No",
                                1: "Yes",
                                pd.NA: "Missing"},
            "ethnicity_prefernot": {0: "No",
                                    1: "Yes",
                                    pd.NA: "Missing"},
            "disability": {1: "No",
                           2: "Yes",
                           99: "Prefer not to answer",
                           pd.NA: "Missing"},
            "physical_activity": {1: "I rarely or never do any physical activity",
                                  2: "I do some light or moderate physical activities",
                                  3: "I do some vigorous physical activities",
                                  99: "Prefer not to answer",
                                  pd.NA: "Missing"},
            "transit_freq": {1: "6-7 days a week",
                             2: "4-5 days a week",
                             3: "2-3 days a week",
                             4: "1 day a week",
                             5: "1-3 days per month",
                             6: "Less than monthly",
                             7: "Never",
                             pd.NA: "Missing"},
            "transitpass": {1: "Monthly Adult Regional Compass Card: $72",
                            2: "Monthly Adult Premium Compass Card: $100",
                            3: "Monthly Adult COASTER Compass Card: by zone",
                            4: "MTS College Semester Pass",
                            5: "MTS College Monthly Pass",
                            6: "UC San Diego Annual U-Pass",
                            7: "Monthly Youth Regional Compass Card: $36",
                            8: "Monthly Youth Premium Compass Card: $50",
                            9: "Monthly Youth COASTER Compass Card: $82.50",
                            10: "Monthly Senior/Disabled/Medicare Regional Compass Card: $18",
                            11: "Monthly Senior/Disabled/Medicare Premium Compass Card: $25",
                            12: "Monthly Senior/Disabled/Medicare COASTER Compass Card: $41.25",
                            13: "Other transit pass (e.g., free, employee, etc.)",
                            14: "Do not have a transit pass",
                            98: "Do not know",
                            pd.NA: "Missing"},
            "schooltype": {1: "Cared for at home",
                           2: "Daycare outside home",
                           3: "Preschool",
                           4: "Kindergarten-Grade 5 (public or private)",
                           5: "Kindergarten-Grade 5 (home school)",
                           6: "Grade 6-Grade 8 (public or private)",
                           7: "Grade 6-Grade 8 (home school)",
                           8: "Grade 9-Grade 12 (public or private)",
                           9: "Grade 9-Grade 12 (home school)",
                           10: "Vocational/technical school",
                           11: "2-year college",
                           12: "4-year college",
                           13: "Graduate or professional school",
                           97: "Other",
                           pd.NA: "Missing"},
            "school_freq": {1: "6-7 days a week",
                            2: "5 days a week",
                            3: "3-4 days a week",
                            4: "1-2 days a week",
                            5: "1-3 days per month",
                            6: "Less than monthly",
                            7: "Never, only takes online classes",
                            pd.NA: "Missing"},
            "other_school": {1: "Never, only 1 school location",
                             2: "1 or more days a week",
                             3: "A few times per month",
                             4: "Less than monthly",
                             pd.NA: "Missing"},
            "school_mode": {1: "Drive alone",
                            2: "Carpool with only family/household member(s)",
                            3: "Carpool with at least one person not in household",
                            4: "Motorcycle/moped/scooter",
                            5: "Walk/jog/wheelchair",
                            6: "Bicycle",
                            7: "School bus",
                            8: "Bus (public transit)",
                            9: "Private shuttle bus",
                            10: "Vanpool",
                            11: "Light Rail (e.g., Trolley, SPRINTER)",
                            12: "Intercity Rail (e.g., COASTER, Amtrak)",
                            13: "Paratransit",
                            14: "Taxi or other hired car service (e.g., Lyft, Uber)",
                            97: "Other",
                            pd.NA: "Missing"},
            "daycare_early": {1: "Before 6 AM",
                              2: "6:00 AM",
                              3: "6:15 AM",
                              4: "6:30 AM",
                              5: "6:45 AM",
                              6: "7:00 AM",
                              7: "7:15 AM",
                              8: "7:30 AM",
                              9: "7:45 AM",
                              10: "8:00 AM",
                              11: "8:15 AM",
                              12: "8:30 AM",
                              13: "After 8:30 AM",
                              pd.NA: "Missing"},
            "daycare_late": {1: "Before 5 PM",
                             2: "5:00 PM",
                             3: "5:15 PM",
                             4: "5:30 PM",
                             5: "5:45 PM",
                             6: "6:00 PM",
                             7: "6:15 PM",
                             8: "6:30 PM",
                             9: "6:45 PM",
                             10: "7:00 PM",
                             11: "7:15 PM",
                             12: "7:30 PM",
                             13: "After 7:30 PM",
                             pd.NA: "Missing"},
            "job_type": {1: "Has one work location (outside of home, may also telework)",
                         2: "Work location regularly varies (work in different offices or jobsites)",
                         3: "Work at home only (only telework or self-employed)",
                         4: "Drive/Travel for a living (e.g., bus/truck driver, salesman)",
                         pd.NA: "Missing"},
            "occupation": {11: "Management Occupations",
                           13: "Business & Financial Operations",
                           15: "Computer & Mathematical",
                           17: "Architecture & Engineering",
                           19: "Life, Physical, & Social Science",
                           21: "Community & Social Services",
                           23: "Legal",
                           25: "Education, Training, & Library",
                           27: "Arts, Design, Entertainment, Sports, & Media",
                           29: "Healthcare Practitioners & Technical",
                           31: "Healthcare Support",
                           33: "Protective Service",
                           35: "Food Preparation & Serving Related",
                           37: "Building & Grounds Cleaning/Maintenance",
                           39: "Personal Care & Service",
                           41: "Sales & Related",
                           43: "Office & Administrative Support",
                           45: "Farming, Fishing, & Forestry",
                           47: "Construction & Extraction",
                           49: "Installation, Maintenance, & Repair",
                           51: "Production",
                           53: "Transportation & Material Moving",
                           55: "Military",
                           97: "Other",
                           98: "Do not know",
                           pd.NA: "Missing"},
            "industry": {1: "Accommodation (e.g., hotels/motels)",
                         2: "Administrative, Support, & Waste Management Services",
                         3: "Agriculture, Forestry, Fishing, & Hunting",
                         4: "Arts, Entertainment, & Recreation",
                         5: "Construction",
                         6: "Education Services",
                         7: "Food Services & Drinking Places",
                         8: "Finance & Insurance",
                         9: "Health Care & Social Assistance",
                         10: "Information",
                         11: "Management of Companies & Enterprises",
                         12: "Manufacturing",
                         13: "Military",
                         14: "Mining, Quarrying, & Oil/Gas Extraction",
                         15: "Other Services",
                         16: "Professional, Scientific, & Technical Services",
                         17: "Public Administration",
                         18: "Real Estate, Rental, & Leasing",
                         19: "Retail Trade",
                         20: "Transportation & Warehousing",
                         21: "Utilities",
                         22: "Wholesale Trade",
                         97: "Other",
                         98: "Do not know",
                         pd.NA: "Missing"},
            "hours_work": {1: "50 or more hours",
                           2: "40-49 hours",
                           3: "35-39 hours",
                           4: "30-34 hours",
                           5: "20-29 hours",
                           6: "10-19 hours",
                           7: "Fewer than 10 hours",
                           8: "Hours vary greatly from week to week",
                           pd.NA: "Missing"},
            "commute_freq": {1: "6-7 days a week",
                             2: "5 days a week",
                             3: "4 days a week",
                             4: "2-3 days a week",
                             5: "1 day a week",
                             6: "9 days every 2 weeks",
                             7: "1-3 days per month",
                             8: "Less than monthly",
                             9: "Never",
                             pd.NA: "Missing"},
            "commute_mode": {1: "Drive alone",
                             2: "Carpool with only family/household member(s)",
                             3: "Carpool with at least one person not in household",
                             4: "Motorcycle/moped/scooter",
                             5: "Walk/jog/wheelchair",
                             6: "Bicycle",
                             7: "School bus",
                             8: "Bus (public transit)",
                             9: "Private shuttle bus",
                             10: "Vanpool",
                             11: "Light Rail (e.g., Trolley, SPRINTER)",
                             12: "Intercity Rail (e.g., COASTER, Amtrak)",
                             13: "Paratransit",
                             14: "Taxi or other hired car service (e.g., Lyft, Uber)",
                             97: "Other",
                             pd.NA: "Missing"},
            "work_flex": {1: "No flexibility (must always arrive on time)",
                          2: "Can arrive up to 15 minutes earlier/later",
                          3: "Can arrive up to 30 minutes earlier/later",
                          4: "Can arrive up to 45 minutes earlier/later",
                          5: "Can arrive more than an hour earlier/later",
                          6: "Sets own schedule (start time can vary greatly)",
                          pd.NA: "Missing"},
            "work_park_pay": {1: "No cost to anyone to park at/near work",
                              2: "Employer pays all parking costs",
                              3: "Employer offers discounted monthly parking pass",
                              4: "Employer offers discounted other (e.g., daily, weekly) parking pass",
                              5: "Personally pay all cost for monthly parking pass",
                              6: "Personally pay all cost for daily parking",
                              7: "Personally pay for parking on other (daily, biweekly, annual) schedule",
                              96: "Not Applicable",
                              98: "Do not know",
                              pd.NA: "Missing"},
            "work_park_cost_dk": {0: "No",
                                  1: "Yes",
                                  pd.NA: "Not Applicable"},
            "work_park_ease": {1: "Easy to find a parking spot",
                               2: "Difficult to find a parking spot (usually takes a few minutes)",
                               96: "Not Applicable",
                               pd.NA: "Missing"},
            "telecommute_freq": {1: "6-7 days a week",
                                 2: "5 days a week",
                                 3: "4 days a week",
                                 4: "2-3 days a week",
                                 5: "1 day a week",
                                 6: "9 days every 2 weeks",
                                 7: "1-3 days per month",
                                 8: "Less than monthly",
                                 9: "Never",
                                 pd.NA: "Missing"},
            "commute_subsidy_none": {0: "No",
                                     1: "Yes",
                                     pd.NA: "Missing"},
            "commute_subsidy_parking": {0: "No",
                                        1: "Yes",
                                        pd.NA: "Missing"},
            "commute_subsidy_transit": {0: "No",
                                        1: "Yes",
                                        pd.NA: "Missing"},
            "commute_subsidy_vanpool": {0: "No",
                                        1: "Yes",
                                        pd.NA: "Missing"},
            "commute_subsidy_cash": {0: "No",
                                     1: "Yes",
                                     pd.NA: "Missing"},
            "commute_subsidy_other": {0: "No",
                                      1: "Yes",
                                      pd.NA: "Missing"},
            "secondhome": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "smartphone_type": {1: "Yes, has an Android phone",
                                2: "Yes, has an iPhone",
                                3: "Yes, has a Windows Phone",
                                4: "Yes, has a Blackberry",
                                5: "Yes, has other type of smartphone",
                                6: "No, does not have a smartphone",
                                99: "Not Applicable",
                                pd.NA: "Missing"},
            "smartphone_age": {1: "Yes",
                               2: "No",
                               99: "Not Applicable",
                               pd.NA: "Missing"},
            "child_smartphone": {1: "Yes",
                                 2: "No",
                                 99: "Not Applicable",
                                 pd.NA: "Missing"},
            "callcenter_diary": {0: "No",
                                 1: "Yes",
                                 99: "Not Applicable",
                                 pd.NA: "Missing"},
            "mobile_diary": {0: "No",
                             1: "Yes",
                             99: "Not Applicable",
                             pd.NA: "Missing"},
            "day1_complete": {0: "No",
                              1: "Yes",
                              pd.NA: "Not Applicable"},
            "day2_complete": {0: "No",
                              1: "Yes",
                              pd.NA: "Not Applicable"},
            "day3_complete": {0: "No",
                              1: "Yes",
                              pd.NA: "Not Applicable"},
            "day4_complete": {0: "No",
                              1: "Yes",
                              pd.NA: "Not Applicable"},
            "day5_complete": {0: "No",
                              1: "Yes",
                              pd.NA: "Not Applicable"},
            "day6_complete": {0: "No",
                              1: "Yes",
                              pd.NA: "Not Applicable"},
            "day7_complete": {0: "No",
                              1: "Yes",
                              pd.NA: "Not Applicable"}
        }

        for field in mappings:
            # define using pd.Categorical to maintain defined category order
            # without setting ordered parameter to True
            df[field] = pd.Categorical(
                df[field].map(mappings[field]),
                categories=mappings[field].values()
            )

            # add Not Applicable category if it does not already exist
            # this is added here for upcoming manual recode operations
            if "Not Applicable" not in df[field].cat.categories:
                df[field].cat.add_categories(["Not Applicable"], inplace=True)

        # manual recodes for age 16+ variables
        df.loc[df.age.isin(["Under 5 years old", "5-15 years"]),
               ["employment",
                "jobs_count",
                "license",
                "military",
                "ethnicity_amindian_alaska",
                "ethnicity_asian",
                "ethnicity_black",
                "ethnicity_hispanic",
                "ethnicity_hawaiian_pacific",
                "ethnicity_white",
                "ethnicity_other",
                "ethnicity_prefernot",
                "disability",
                "transit_freq",
                "transitpass",
                "job_type",
                "occupation",
                "industry",
                "hours_work",
                "commute_freq",
                "commute_mode",
                "work_flex",
                "work_park_pay",
                "work_park_cost_dk",
                "work_park_ease",
                "telecommute_freq",
                "commute_subsidy_none",
                "commute_subsidy_parking",
                "commute_subsidy_transit",
                "commute_subsidy_vanpool",
                "commute_subsidy_cash",
                "commute_subsidy_other",
                "commute_subsidy_specify",
                "work_address",
                "secondwork_address",
                "smartphone_type",
                "smartphone_age"]] = "Not Applicable"

        # manual recode for age 16-17 variables
        df.loc[df.age != "16-17 years", "child_smartphone"] = "Not Applicable"

        # manual recodes for age 18+ variables
        df.loc[df.age.isin(["Under 5 years old", "5-15 years", "16-17 years"]),
               ["student",
                "education",
                "physical_activity"]] = "Not Applicable"

        # manual recode for employed variables
        df.loc[df.employment == "Not currently employed",
               ["jobs_count",
                "military",
                "job_type",
                "occupation",
                "industry",
                "hours_work",
                "commute_freq",
                "commute_mode",
                "work_flex",
                "work_park_pay",
                "work_park_cost_dk",
                "work_park_ease",
                "telecommute_freq",
                "commute_subsidy_none",
                "commute_subsidy_parking",
                "commute_subsidy_transit",
                "commute_subsidy_vanpool",
                "commute_subsidy_cash",
                "commute_subsidy_other",
                "commute_subsidy_specify",
                "work_address",
                "secondwork_address"]] = "Not Applicable"

        # no way to differentiate between those with a second work
        # that have second work address missing and those who do not
        # have a second work, just assume Not Applicable if missing
        df["secondwork_address"] = df["secondwork_address"].fillna("Not Applicable")

        # manual recode for ethnicity variables
        df.loc[df.ethnicity_prefernot == "Yes",
               ["ethnicity_amindian_alaska",
                "ethnicity_asian",
                "ethnicity_black",
                "ethnicity_hispanic",
                "ethnicity_hawaiian_pacific",
                "ethnicity_white",
                "ethnicity_other"]] = "Not Applicable"

        # manual recode for transit pass variable
        df.loc[df.transit_freq.isin(["1-3 days per month",
                                     "Less than monthly",
                                     "Never"]),
               ["transitpass"]] = "Not Applicable"

        # manual recode for school address variables
        df.loc[df.schooltype.isin(["Missing", "Not Applicable"]),
               ["mainschool_address",
                "secondschool_address"]] = "Not Applicable"

        # no indicator if second school exists, assume Not Applicable if missing
        df["secondschool_address"] = df["secondschool_address"].fillna("Not Applicable")

        # manual recode for school commute variables
        df.loc[df.schooltype.isin(["Cared for at home",
                                   "Kindergarten-Grade 5 (home school)",
                                   "Grade 6-Grade 8 (home school)",
                                   "Grade 9-Grade 12 (home school)",
                                   "Missing",
                                   "Not Applicable"]),
               ["school_freq",
                "other_school",
                "school_mode"]] = "Not Applicable"

        df.loc[df.school_freq == "Never, only takes online classes",
               "school_mode"] = "Not Applicable"

        # manual recode for daycare variables
        df.loc[df.schooltype != "Daycare outside home",
               ["daycare_early",
                "daycare_late"]] = "Not Applicable"

        # manual recode for non-military variables
        df.loc[df.military.isin(["Active duty within the San Diego region",
                                 "Active duty outside of the San Diego region"]),
               ["job_type",
                "occupation",
                "industry",
                "hours_work",
                "commute_freq",
                "commute_mode",
                "work_flex",
                "work_park_pay",
                "work_park_cost_dk",
                "work_park_ease",
                "telecommute_freq",
                "commute_subsidy_none",
                "commute_subsidy_parking",
                "commute_subsidy_transit",
                "commute_subsidy_vanpool",
                "commute_subsidy_cash",
                "commute_subsidy_other",
                "commute_subsidy_specify"]] = "Not Applicable"

        # manual recode for commute variables
        df.loc[df.job_type == "Work at home only (only telework or self-employed)",
               ["telecommute_freq"]] = "Not Applicable"

        df.loc[df.job_type.isin(["Work at home only (only telework or self-employed)",
                                 "Drive/Travel for a living (e.g., bus/truck driver, salesman)"]),
               ["commute_freq"]] = "Not Applicable"

        df.loc[df.commute_freq.isin(["Never",
                                     "Not Applicable"]),
               ["commute_mode",
                "work_flex",
                "work_park_pay",
                "work_park_cost_dk",
                "work_park_ease",
                "commute_subsidy_none",
                "commute_subsidy_parking",
                "commute_subsidy_transit",
                "commute_subsidy_vanpool",
                "commute_subsidy_cash",
                "commute_subsidy_other",
                "commute_subsidy_specify"]] = "Not Applicable"

        df.loc[df.commute_subsidy_other.isin(["No",
                                              "Not Applicable"]),
               "commute_subsidy_specify"] = "Not Applicable"

        # manual recode for work parking payment variables
        df.loc[~df.commute_mode.isin(["Drive alone",
                                      "Carpool with only family/household member(s)",
                                      "Carpool with at least one person not in household",
                                      "Motorcycle/moped/scooter"]),
               ["work_park_pay",
                "work_park_cost_dk",
                "work_park_ease"]] = "Not Applicable"

        # manual recode for second home address variable
        df.loc[df.secondhome != "Yes", "secondhome_address"] = "Not Applicable"

        df.loc[df.rmove_participant == "Yes",
               ["callcenter_diary",
                "mobile_diary"]] = "Not Applicable"

        # manual recode for smartphone_age variable
        df.loc[df.smartphone_type.isin(["Yes, has a Windows Phone",
                                        "Yes, has other type of smartphone",
                                        "Yes, has a Blackberry",
                                        "No, does not have a smartphone",
                                        "Not Applicable"]),
               "smartphone_age"] = "Not Applicable"

        # for address variables not already set to Not Applicable
        # set missing values to explicit Missing
        df["mainschool_address"] = df["mainschool_address"].fillna("Missing")
        df["work_address"] = df["work_address"].fillna("Missing")

        # create WKT point geometries from lat/long in EPSG:2230
        df["second_home_shape"] = self.point_wkt(
            coordinates=zip(df["secondhome_lng"], df["secondhome_lat"]),
            crs="EPSG:2230"
        )

        df["school_shape"] = self.point_wkt(
            coordinates=zip(df["mainschool_lng"], df["mainschool_lat"]),
            crs="EPSG:2230"
        )

        df["second_school_shape"] = self.point_wkt(
            coordinates=zip(df["secondschool_lng"], df["secondschool_lat"]),
            crs="EPSG:2230"
        )

        df["work_shape"] = self.point_wkt(
            coordinates=zip(df["work_lng"], df["work_lat"]),
            crs="EPSG:2230"
        )

        df["second_work_shape"] = self.point_wkt(
            coordinates=zip(df["secondwork_lng"], df["secondwork_lat"]),
            crs="EPSG:2230"
        )

        # rename columns
        df.rename(columns={
            "personid": "person_id",
            "hhid": "household_id",
            "pernum": "person_number",
            "traveldate_start": "travel_date_start",
            "age": "age_category",
            "employment": "employment_status",
            "jobs_count": "number_of_jobs",
            "student": "adult_student_status",
            "education": "educational_attainment",
            "license": "drivers_license",
            "military": "military_status",
            "ethnicity_amindian_alaska": "ethnicity_americanindian_alaskanative",
            "weight_lbs": "weight",
            "transit_freq": "transit_frequency",
            "transitpass": "transit_pass",
            "schooltype": "school_type",
            "school_freq": "school_frequency",
            "daycare_early": "daycare_open",
            "daycare_late": "daycare_close",
            "job_type": "work_location_type",
            "hours_work": "hours_worked",
            "commute_freq": "commute_frequency",
            "work_flex": "work_arrival_frequency",
            "work_park_pay": "work_parking_payment",
            "work_park_cost": "work_parking_cost",
            "work_park_cost_dk": "work_parking_cost_dk",
            "work_park_ease": "work_parking_ease",
            "telecommute_freq": "telecommute_frequency",
            "secondhome": "has_second_home",
            "secondhome_address": "second_home_address",
            "secondhome_lat": "second_home_latitude",
            "secondhome_lng": "second_home_longitude",
            "mainschool_address": "school_address",
            "mainschool_lat": "school_latitude",
            "mainschool_lng": "school_longitude",
            "secondschool_address": "second_school_address",
            "secondschool_lat": "second_school_latitude",
            "secondschool_lng": "second_school_longitude",
            "work_lat": "work_latitude",
            "work_lng": "work_longitude",
            "secondwork_address": "second_work_address",
            "secondwork_lat": "second_work_latitude",
            "secondwork_lng": "second_work_longitude",
            "child_smartphone": "smartphone_child",
            "callcenter_diary": "diary_callcenter",
            "mobile_diary": "diary_mobile",
            "activated_rmove": "rmove_activated",
            "numdays_complete": "completed_days",
            "day1_complete": "completed_day1",
            "day2_complete": "completed_day2",
            "day3_complete": "completed_day3",
            "day4_complete": "completed_day4",
            "day5_complete": "completed_day5",
            "day6_complete": "completed_day6",
            "day7_complete": "completed_day7"
        },
            inplace=True)

        return df[["person_id",
                   "household_id",
                   "person_number",
                   "travel_date_start",
                   "rmove_participant",
                   "relationship",
                   "gender",
                   "age_category",
                   "employment_status",
                   "number_of_jobs",
                   "adult_student_status",
                   "educational_attainment",
                   "drivers_license",
                   "military_status",
                   "ethnicity_americanindian_alaskanative",
                   "ethnicity_asian",
                   "ethnicity_black",
                   "ethnicity_hispanic",
                   "ethnicity_hawaiian_pacific",
                   "ethnicity_white",
                   "ethnicity_other",
                   "disability",
                   "height",
                   "weight",
                   "physical_activity",
                   "transit_frequency",
                   "transit_pass",
                   "school_type",
                   "school_frequency",
                   "other_school",
                   "school_mode",
                   "daycare_open",
                   "daycare_close",
                   "work_location_type",
                   "occupation",
                   "industry",
                   "hours_worked",
                   "commute_frequency",
                   "commute_mode",
                   "work_arrival_frequency",
                   "work_parking_payment",
                   "work_parking_cost",
                   "work_parking_cost_dk",
                   "work_parking_ease",
                   "telecommute_frequency",
                   "commute_subsidy_none",
                   "commute_subsidy_parking",
                   "commute_subsidy_transit",
                   "commute_subsidy_vanpool",
                   "commute_subsidy_cash",
                   "commute_subsidy_other",
                   "commute_subsidy_specify",
                   "has_second_home",
                   "second_home_address",
                   "second_home_latitude",
                   "second_home_longitude",
                   "second_home_shape",
                   "school_address",
                   "school_latitude",
                   "school_longitude",
                   "school_shape",
                   "second_school_address",
                   "second_school_latitude",
                   "second_school_longitude",
                   "second_school_shape",
                   "work_address",
                   "work_latitude",
                   "work_longitude",
                   "work_shape",
                   "second_work_address",
                   "second_work_latitude",
                   "second_work_longitude",
                   "second_work_shape",
                   "smartphone_type",
                   "smartphone_age",
                   "smartphone_child",
                   "diary_callcenter",
                   "diary_mobile",
                   "rmove_activated",
                   "completed_days",
                   "completed_day1",
                   "completed_day2",
                   "completed_day3",
                   "completed_day4",
                   "completed_day5",
                   "completed_day6",
                   "completed_day7"]]

    @property
    @lru_cache(maxsize=1)
    def trips(self) -> pd.DataFrame:
        """ Person trip list containing the following columns:
                trip_id - unique identifier of trip
                trip_id_linked - linked trip identifier
                trip_id_location - trip identifier for joining to location table
                person_id - unique identifier of person
                household_id - unique identifier of household
                travel_date - date of travel
                travel_day_number - travel day number for person (1-7)
                travel_day_of_week - travel day of week
                data_source - trip was recorded by rMove or online
                completed_trip_survey - trip survey is complete
                completed_date - rMove trip; date and time survey was completed
                completed_household_survey - all household surveys completed on travel date of trip
                completed_person_survey - all surveys completed by person on travel date of trip
                number_household_survey_weekdays - number of complete weekdays for household
                revised_at - rMove trip; Date and time survey was last revised
                revised_count - rMove trip; Number of revisions to survey
                error - rMove trip; user reported error on trip
                flag_teleport - rMove trip; >=250m gap occurs after trip
                copied_trip - trip copied from another household member
                analyst_merged_trip - rMove trip; trip is result of 2 or more trips merged by analyst
                analyst_split_trip - rMove trip; trip is result of trip split by analyst
                user_merged_trip - rMove trip; trip is result of 2 or more trips merged by user
                user_split_trip - rMove trip; trip is result of trip split by user
                added_trip - rMove trip; trip was added by user
                nonproxy_derived_trip - rMove trip; trip was derived from a non-proxy record for a non-participant
                proxy_added_trip - rMove trip; trip was added by proxy respondent
                unlinked_transit_trip - trip is part of an unlinked transit trip
                origin_name - online trip; origin name
                origin_address - online trip; origin address
                origin_latitude - origin latitude
                origin_longitude - origin longitude
                origin_shape - origin point geometry (EPSG: 2230)
                destination_name - online trip; destination name
                destination_address - online trip; destination address
                destination_latitude - destination latitude
                destination_longitude - destination longitude
                destination_shape - destination point geometry (EPSG:2230)
                origin_purpose - derived origin purpose
                origin_purpose_other_specify - derived other origin purpose
                origin_purpose_inferred - inferred origin purpose based on home, work, and school locations
                destination_purpose - derived destination purpose
                destination_purpose_other_specify - derived other destination purpose
                destination_purpose_inferred - inferred destination purpose based on home, work, and school locations
                departure_time - departure time
                arrival_time - arrival time
                travelers - derived number of people on trip, including self
                travelers_household - derived number of household members on trip, including self
                travelers_non_household - number of non-household members on trip
                mode_1 - travel mode (up to four allowed on a trip)
                mode_2 - travel mode (up to four allowed on a trip)
                mode_3 - travel mode (up to four allowed on a trip)
                mode_4 - travel mode (up to four allowed on a trip)
                mode_transit_access - online trip, bus or rail; access mode
                mode_transit_egress - online trip, bus or rail; egress mode
                google_mode - unlinked transit trip; google-suggested mode category for trip leg
                driver - auto trip, non-taxi; Driver or passenger on trip
                toll_road - online trip, auto trip; used toll road
                toll_road_express - online trip, auto trip; used toll road with express lane
                parking_location - auto trip, non-taxi; parking location
                parking_pay_type - parked on street/lot; how paid for parking
                parking_cost - paid for parking; cost of parking ($)
                parking_cost_dk - paid for parking; does not know cost of parking
                parking_egress_duration - rMove, parked on street/lot; walking duration from parking spot
                    to destination (minutes)
                taxi_pay_type - taxi trip; how paid for taxi
                taxi_cost - taxi trip, paid or was reimbursed; cost of taxi fare ($)
                taxi_cost_dk - taxi trip, paid or was reimbursed; does not know cost of taxi fare
                airplane_pay_type - airplane trip; how paid for airfare
                airplane_cost - airplane trip, paid; cost of airfare ($)
                airplane_cost_dk - airplane trip, paid; does not know cost of airfare
                bus_pay_type - bus trip; how paid for fare
                bus_cost - bus trip, paid; cost of fare ($)
                bus_cost_dk - bus trip, paid; does not know cost of fare
                rail_pay_type - rail trip; how paid for fare
                rail_cost - rail trip, paid; cost of fare ($)
                rail_cost_dk - rail trip, paid; does not know cost of fare
                ferry_pay_type - ferry trip; how paid for fare
                ferry_cost - ferry trip, paid; cost of fare ($)
                ferry_cost_dk - ferry trip, paid; does not know cost of fare
                parkride_lot - online trip, parked in Park& Ride lot; which lot
                parkride_city - online trip, parked in Park & Ride lot; city where lot is located
                distance - derived trip path distance (miles)
                duration - derived trip duration (minutes)
                duration_reported - online trip: reported trip duration (minutes)
                speed - rMove trip: derived trip speed (mph)
                weight_trip - weight to use for non-weighted survey trip based metrics
                weight_person_trip - weight to use for non-weighted survey person trip based metrics
                weight_household_multiday_factor - household multi-day weight factor
                weight_person_multiday_456x - factored person-day weight """

        # load AT data-set
        at_df = pd.read_csv(
            "../data/at/SDRTS_AT_Trip_Data_20170831.csv",
            usecols=["tripid",
                     "tripid_linked",
                     # "location_tripid",
                     "personid",
                     "hhid",
                     "traveldate",
                     "daynum",
                     "travel_dow",
                     # "data_source",
                     "svy_complete",
                     "completed_at",
                     # "day_hhcomplete",
                     # "pday_complete",
                     # "h_complete_weekdays",
                     "revised_at",
                     "revised_count",
                     "error",
                     "flag_teleport",
                     "copied_trip",
                     "analyst_merged",
                     "analyst_split",
                     "user_merged",
                     "user_split",
                     "added_trip",
                     # "nonproxy_derived_trip",
                     # "proxy_added_trip",
                     "unlinked_transit_trip",
                     # "origin_name",
                     # "origin_address",
                     "origin_lat",
                     "origin_lng",
                     # "destination_name",
                     # "destination_address",
                     "destination_lat",
                     "destination_lng",
                     "o_purpose",
                     "o_purpose_other",
                     "o_purpose_inferred",
                     "d_purpose",
                     "d_purpose_other",
                     "d_purpose_inferred",
                     "departure_time",
                     "arrival_time",
                     "travelers_total",
                     "travelers_hh",
                     "travelers_nonhh",
                     "mode1",
                     # "mode2",
                     # "mode3",
                     # "mode4",
                     # "transit_access",
                     # "transit_egress",
                     "google_mode",
                     "driver",
                     # "toll_no",
                     # "toll_express",
                     "parklocation",
                     "parktype",
                     "parkcost",
                     "park_cost_dk",
                     "parkegress_time",
                     "taxitype",
                     "taxicost",
                     "taxi_cost_dk",
                     "airtype",
                     "airfarecost",
                     "airfare_cost_dk",
                     "bustype",
                     "buscost",
                     "bus_cost_dk",
                     "railtype",
                     "railcost",
                     "rail_cost_dk",
                     "ferrytype",
                     "ferrycost",
                     "ferry_cost_dk",
                     # "parkride_lot",
                     # "parkride_city",
                     "trip_path_distance",
                     "trip_duration",
                     # "trip_duration_reported",
                     "speed_mph"
                     # "h_multiday_factor",
                     # "multiday_weight_456x"]
                     ],
            dtype={"travel_dow": "Int8",
                   # "data_source": "Int8",
                   "svy_complete": "Int8",
                   # "day_hhcomplete": "Int8",
                   # "pday_complete": "Int8",
                   "error": "Int8",
                   "flag_teleport": "Int8",
                   "copied_trip": "Int8",
                   "analyst_merged": "Int8",
                   "analyst_split": "Int8",
                   "user_merged": "Int8",
                   "user_split": "Int8",
                   "added_trip": "Int8",
                   # "nonproxy_derived_trip": "Int8",
                   # "proxy_added_trip": "Int8",
                   "unlinked_transit_trip": "Int8",
                   "o_purpose": "Int16",
                   "o_purpose_inferred": "Int16",
                   "d_purpose": "Int16",
                   "d_purpose_inferred": "Int16",
                   "travelers_total": "Int16",
                   "travelers_hh": "Int16",
                   "travelers_nonhh": "Int16",
                   "mode1": "Int16",
                   "mode2": "Int16",
                   "mode3": "Int16",
                   "mode4": "Int16",
                   # "transit_access": "Int8",
                   # "transit_egress": "Int8",
                   "driver": "Int8",
                   # "toll_no": "Int8",
                   # "toll_express": "Int8",
                   "parklocation": "Int16",
                   "parktype": "Int16",
                   "park_cost_dk": "Int8",
                   "taxitype": "Int16",
                   "taxi_cost_dk": "Int8",
                   "airtype": "Int16",
                   "airfare_cost_dk": "Int8",
                   "bustype": "Int16",
                   "bus_cost_dk": "Int8",
                   "railtype": "Int16",
                   "rail_cost_dk": "Int8",
                   "ferrytype": "Int16",
                   "ferry_cost_dk": "Int8",
                   "parkride_lot": "Int8",
                   "parkride_city": "Int8"}
        )

        # set values for variables not in AT data-set
        at_df["location_tripid"] = at_df["tripid_linked"]
        at_df["data_source"] = 1  # all rMoves
        at_df["day_hhcomplete"] = 99  # new hardcoded Not Applicable value
        at_df["pday_complete"] = 99  # new hardcoded Not Applicable value
        at_df["nonproxy_derived_trip"] = 99  # new hardcoded Not Applicable value
        at_df["proxy_added_trip"] = 99  # new hardcoded Not Applicable value
        at_df["origin_name"] = "Not Applicable"
        at_df["origin_address"] = "Not Applicable"
        at_df["destination_name"] = "Not Applicable"
        at_df["destination_address"] = "Not Applicable"
        at_df["transit_access"] = 99  # new hardcoded Not Applicable value
        at_df["transit_egress"] = 99  # new hardcoded Not Applicable value
        at_df["toll_no"] = 99  # new hardcoded Not Applicable value
        at_df["toll_express"] = 99  # new hardcoded Not Applicable value

        # combine HHTBS and AT data-sets
        df = pd.concat(
            [
                pd.read_csv(
                    "../data/sdrts/SDRTS_Trip_Data_20170731.csv",
                    usecols=["tripid",
                             "tripid_linked",
                             "location_tripid",
                             "personid",
                             "hhid",
                             "traveldate",
                             "daynum",
                             "travel_dow",
                             "data_source",
                             "svy_complete",
                             "completed_at",
                             "day_hhcomplete",
                             "pday_complete",
                             "h_complete_weekdays",
                             "revised_at",
                             "revised_count",
                             "error",
                             "flag_teleport",
                             "copied_trip",
                             "analyst_merged",
                             "analyst_split",
                             "user_merged",
                             "user_split",
                             "added_trip",
                             "nonproxy_derived_trip",
                             "proxy_added_trip",
                             "unlinked_transit_trip",
                             "origin_name",
                             "origin_address",
                             "origin_lat",
                             "origin_lng",
                             "destination_name",
                             "destination_address",
                             "destination_lat",
                             "destination_lng",
                             "o_purpose",
                             "o_purpose_other",
                             "o_purpose_inferred",
                             "d_purpose",
                             "d_purpose_other",
                             "d_purpose_inferred",
                             "departure_time",
                             "arrival_time",
                             "travelers_total",
                             "travelers_hh",
                             "travelers_nonhh",
                             "mode1",
                             "mode2",
                             "mode3",
                             "mode4",
                             "transit_access",
                             "transit_egress",
                             "google_mode",
                             "driver",
                             "toll_no",
                             "toll_express",
                             "parklocation",
                             "parktype",
                             "parkcost",
                             "park_cost_dk",
                             "parkegress_time",
                             "taxitype",
                             "taxicost",
                             "taxi_cost_dk",
                             "airtype",
                             "airfarecost",
                             "airfare_cost_dk",
                             "bustype",
                             "buscost",
                             "bus_cost_dk",
                             "railtype",
                             "railcost",
                             "rail_cost_dk",
                             "ferrytype",
                             "ferrycost",
                             "ferry_cost_dk",
                             "parkride_lot",
                             "parkride_city",
                             "trip_path_distance",
                             "trip_duration",
                             "trip_duration_reported",
                             "speed_mph",
                             "h_multiday_factor",
                             "multiday_weight_456x"],
                    dtype={"travel_dow": "Int8",
                           "data_source": "Int8",
                           "svy_complete": "Int8",
                           "day_hhcomplete": "Int8",
                           "pday_complete": "Int8",
                           "error": "Int8",
                           "flag_teleport": "Int8",
                           "copied_trip": "Int8",
                           "analyst_merged": "Int8",
                           "analyst_split": "Int8",
                           "user_merged": "Int8",
                           "user_split": "Int8",
                           "added_trip": "Int8",
                           "nonproxy_derived_trip": "Int8",
                           "proxy_added_trip": "Int8",
                           "unlinked_transit_trip": "Int8",
                           "o_purpose": "Int16",
                           "o_purpose_inferred": "Int16",
                           "d_purpose": "Int16",
                           "d_purpose_inferred": "Int16",
                           "travelers_total": "Int16",
                           "travelers_hh": "Int16",
                           "travelers_nonhh": "Int16",
                           "mode1": "Int16",
                           "mode2": "Int16",
                           "mode3": "Int16",
                           "mode4": "Int16",
                           "transit_access": "Int8",
                           "transit_egress": "Int8",
                           "driver": "Int8",
                           "toll_no": "Int8",
                           "toll_express": "Int8",
                           "parklocation": "Int16",
                           "parktype": "Int16",
                           "park_cost_dk": "Int8",
                           "taxitype": "Int16",
                           "taxi_cost_dk": "Int8",
                           "airtype": "Int16",
                           "airfare_cost_dk": "Int8",
                           "bustype": "Int16",
                           "bus_cost_dk": "Int8",
                           "railtype": "Int16",
                           "rail_cost_dk": "Int8",
                           "ferrytype": "Int16",
                           "ferry_cost_dk": "Int8",
                           "parkride_lot": "Int8",
                           "parkride_city": "Int8"}
                ),
                at_df
            ],
            ignore_index=True
        )

        # apply exhaustive field mappings where applicable
        mappings = {
            "travel_dow": {1: "Monday",
                           2: "Tuesday",
                           3: "Wednesday",
                           4: "Thursday",
                           5: "Friday",
                           6: "Saturday",
                           7: "Sunday",
                           pd.NA: "Missing"},
            "data_source": {1: "rMove",
                            2: "Online",
                            pd.NA: "Missing"},
            "svy_complete": {0: "No",
                             1: "Yes",
                             pd.NA: "Missing"},
            "day_hhcomplete": {0: "No",
                               1: "Yes",
                               99: "Not Applicable",
                               pd.NA: "Missing"},
            "pday_complete": {0: "No",
                              1: "Yes",
                              99: "Not Applicable",
                              pd.NA: "Missing"},
            "error": {1: "No error",
                      2: "Not moving",
                      3: "Still traveling",
                      4: "Made other stops",
                      97: "Other error",
                      pd.NA: "Missing"},
            "flag_teleport": {0: "No",
                              1: "Yes",
                              pd.NA: "Missing"},
            "copied_trip": {0: "No",
                            1: "Yes",
                            pd.NA: "Missing"},
            "analyst_merged": {0: "No",
                               1: "Yes",
                               pd.NA: "Missing"},
            "analyst_split": {0: "No",
                              1: "Yes",
                              pd.NA: "Missing"},
            "user_merged": {0: "No",
                            1: "Yes",
                            pd.NA: "Missing"},
            "user_split": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "added_trip": {0: "No",
                           1: "Yes",
                           pd.NA: "Missing"},
            "nonproxy_derived_trip": {0: "No",
                                      1: "Yes",
                                      99: "Not Applicable",
                                      pd.NA: "Missing"},
            "proxy_added_trip": {0: "No",
                                 1: "Yes",
                                 99: "Not Applicable",
                                 pd.NA: "Missing"},
            "unlinked_transit_trip": {0: "No",
                                      1: "Yes",
                                      pd.NA: "Missing"},
            "o_purpose": {1: "Home",
                          3: "School/Class",
                          5: "Drop off, pick up, accompany person (Online diary only)",
                          7: "Social/leisure/vacation activity (Online diary only)",
                          10: "Primary workplace",
                          11: "Work-related",
                          12: "Traveling for work (e.g., going to airport)",
                          13: "Volunteer work",
                          14: "Other work",
                          21: "K-12 School",
                          22: "College/University",
                          24: "Other education-related (e.g., field trip)",
                          25: "Vocational education",
                          30: "Grocery",
                          31: "Gas",
                          32: "Routine shopping",
                          33: "Errands without appointment",
                          34: "Medical",
                          36: "Shopping for a major item",
                          37: "Errands with appointment",
                          45: "Pick someone up (rMove only)",
                          46: "Drop someone off (rMove only)",
                          47: "Accompany someone (rMove only)",
                          48: "Multiple: pickup, dropoff, accompany (rMove only)",
                          50: "Restaurant",
                          51: "Exercise",
                          52: "Social (rMove only)",
                          53: "Leisure/entertainment (rMove only)",
                          54: "Religious/civic (rMove only)",
                          55: "Vacation/travel (rMove only)",
                          56: "Family activity (rMove only)",
                          60: "Change travel mode",
                          61: "Other errand",
                          62: "Other leisure (rMove only)",
                          97: "Other purpose",
                          99: "Other",
                          -9999: "Technical error",
                          -9998: "Participant non-response",
                          pd.NA: "Missing"},
            "o_purpose_inferred": {1: "Home",
                                   3: "School/Class",
                                   5: "Drop off, pick up, accompany person (Online diary only)",
                                   7: "Social/leisure/vacation activity (Online diary only)",
                                   10: "Primary workplace",
                                   11: "Work-related",
                                   12: "Traveling for work (e.g., going to airport)",
                                   13: "Volunteer work",
                                   14: "Other work",
                                   21: "K-12 School",
                                   22: "College/University",
                                   24: "Other education-related (e.g., field trip)",
                                   25: "Vocational education",
                                   30: "Grocery",
                                   31: "Gas",
                                   32: "Routine shopping",
                                   33: "Errands without appointment",
                                   34: "Medical",
                                   36: "Shopping for a major item",
                                   37: "Errands with appointment",
                                   45: "Pick someone up (rMove only)",
                                   46: "Drop someone off (rMove only)",
                                   47: "Accompany someone (rMove only)",
                                   48: "Multiple: pickup, dropoff, accompany (rMove only)",
                                   50: "Restaurant",
                                   51: "Exercise",
                                   52: "Social (rMove only)",
                                   53: "Leisure/entertainment (rMove only)",
                                   54: "Religious/civic (rMove only)",
                                   55: "Vacation/travel (rMove only)",
                                   56: "Family activity (rMove only)",
                                   60: "Change travel mode",
                                   61: "Other errand",
                                   62: "Other leisure (rMove only)",
                                   97: "Other purpose",
                                   99: "Other",
                                   -9999: "Technical error",
                                   -9998: "Participant non-response",
                                   pd.NA: "Missing"},
            "d_purpose": {1: "Home",
                          3: "School/Class",
                          5: "Drop off, pick up, accompany person (Online diary only)",
                          7: "Social/leisure/vacation activity (Online diary only)",
                          10: "Primary workplace",
                          11: "Work-related",
                          12: "Traveling for work (e.g., going to airport)",
                          13: "Volunteer work",
                          14: "Other work",
                          21: "K-12 School",
                          22: "College/University",
                          24: "Other education-related (e.g., field trip)",
                          25: "Vocational education",
                          30: "Grocery",
                          31: "Gas",
                          32: "Routine shopping",
                          33: "Errands without appointment",
                          34: "Medical",
                          36: "Shopping for a major item",
                          37: "Errands with appointment",
                          45: "Pick someone up (rMove only)",
                          46: "Drop someone off (rMove only)",
                          47: "Accompany someone (rMove only)",
                          48: "Multiple: pickup, dropoff, accompany (rMove only)",
                          50: "Restaurant",
                          51: "Exercise",
                          52: "Social (rMove only)",
                          53: "Leisure/entertainment (rMove only)",
                          54: "Religious/civic (rMove only)",
                          55: "Vacation/travel (rMove only)",
                          56: "Family activity (rMove only)",
                          60: "Change travel mode",
                          61: "Other errand",
                          62: "Other leisure (rMove only)",
                          97: "Other purpose",
                          99: "Other",
                          -9999: "Technical error",
                          -9998: "Participant non-response",
                          pd.NA: "Missing"},
            "d_purpose_inferred": {1: "Home",
                                   3: "School/Class",
                                   5: "Drop off, pick up, accompany person (Online diary only)",
                                   7: "Social/leisure/vacation activity (Online diary only)",
                                   10: "Primary workplace",
                                   11: "Work-related",
                                   12: "Traveling for work (e.g., going to airport)",
                                   13: "Volunteer work",
                                   14: "Other work",
                                   21: "K-12 School",
                                   22: "College/University",
                                   24: "Other education-related (e.g., field trip)",
                                   25: "Vocational education",
                                   30: "Grocery",
                                   31: "Gas",
                                   32: "Routine shopping",
                                   33: "Errands without appointment",
                                   34: "Medical",
                                   36: "Shopping for a major item",
                                   37: "Errands with appointment",
                                   45: "Pick someone up (rMove only)",
                                   46: "Drop someone off (rMove only)",
                                   47: "Accompany someone (rMove only)",
                                   48: "Multiple: pickup, dropoff, accompany (rMove only)",
                                   50: "Restaurant",
                                   51: "Exercise",
                                   52: "Social (rMove only)",
                                   53: "Leisure/entertainment (rMove only)",
                                   54: "Religious/civic (rMove only)",
                                   55: "Vacation/travel (rMove only)",
                                   56: "Family activity (rMove only)",
                                   60: "Change travel mode",
                                   61: "Other errand",
                                   62: "Other leisure (rMove only)",
                                   97: "Other purpose",
                                   99: "Other",
                                   -9999: "Technical error",
                                   -9998: "Participant non-response",
                                   pd.NA: "Missing"},
            "travelers_total": {1: 1,
                                2: 2,
                                3: 3,
                                4: 4,
                                5: 5,
                                6: 6,
                                7: 7,
                                8: 8,
                                9: 9,
                                10: 10,
                                -9998: "Participant Non-response",
                                -9999: "Technical error",
                                pd.NA: "Missing"},
            "travelers_hh": {1: 1,
                             2: 2,
                             3: 3,
                             4: 4,
                             5: 5,
                             6: 6,
                             7: 7,
                             8: 8,
                             9: 9,
                             10: 10,
                             -9998: "Participant Non-response",
                             -9999: "Technical error",
                             pd.NA: "Missing"},
            "travelers_nonhh": {0: 0,
                                1: 1,
                                2: 2,
                                3: 3,
                                4: 4,
                                5: "5+",
                                -9998: "Participant Non-response",
                                -9999: "Technical error",
                                pd.NA: "Missing"},
            "mode1": {1: "Walk/jog/wheelchair",
                      2: "Personal bicycle",
                      3: "Borrowed bicycle",
                      4: "Rental bicycle",
                      6: "Household vehicle 1",
                      7: "Household vehicle 2",
                      8: "Household vehicle 3",
                      9: "Household vehicle 4",
                      10: "Household vehicle 5",
                      11: "Household vehicle 6",
                      12: "Household vehicle 7",
                      16: "Other household vehicle",
                      17: "Rental car",
                      18: "Carshare",
                      21: "Vanpool",
                      22: "Other auto",
                      23: "Bus",
                      24: "School bus",
                      25: "Intercity bus",
                      26: "Shuttle bus",
                      27: "Paratransit",
                      28: "Other bus",
                      30: "Subway",
                      31: "Airplane",
                      32: "Ferry or water taxi",
                      33: "Work car",
                      34: "Friends car",
                      36: "Taxi - Regular",
                      37: "Taxi - Rideshare",
                      38: "University bus or shuttle",
                      39: "Rail - Light",
                      41: "Rail - Intercity",
                      42: "Rail - Other",
                      43: "Skateboard",
                      44: "Golf cart",
                      45: "ATV",
                      47: "Other household motorcycle",
                      55: "Express bus/Rapid",
                      97: "Other mode",
                      150: "San Diego Coaster Line",
                      -9999: "Technical error",
                      -9998: "Participant non-response",
                      pd.NA: "Not Applicable"},
            "mode2": {1: "Walk/jog/wheelchair",
                      2: "Personal bicycle",
                      3: "Borrowed bicycle",
                      4: "Rental bicycle",
                      6: "Household vehicle 1",
                      7: "Household vehicle 2",
                      8: "Household vehicle 3",
                      9: "Household vehicle 4",
                      10: "Household vehicle 5",
                      11: "Household vehicle 6",
                      12: "Household vehicle 7",
                      16: "Other household vehicle",
                      17: "Rental car",
                      18: "Carshare",
                      21: "Vanpool",
                      22: "Other auto",
                      23: "Bus",
                      24: "School bus",
                      25: "Intercity bus",
                      26: "Shuttle bus",
                      27: "Paratransit",
                      28: "Other bus",
                      30: "Subway",
                      31: "Airplane",
                      32: "Ferry or water taxi",
                      33: "Work car",
                      34: "Friends car",
                      36: "Taxi - Regular",
                      37: "Taxi - Rideshare",
                      38: "University bus or shuttle",
                      39: "Rail - Light",
                      41: "Rail - Intercity",
                      42: "Rail - Other",
                      43: "Skateboard",
                      44: "Golf cart",
                      45: "ATV",
                      47: "Other household motorcycle",
                      55: "Express bus/Rapid",
                      97: "Other mode",
                      150: "San Diego Coaster Line",
                      -9999: "Technical error",
                      -9998: "Participant non-response",
                      pd.NA: "Not Applicable"},
            "mode3": {1: "Walk/jog/wheelchair",
                      2: "Personal bicycle",
                      3: "Borrowed bicycle",
                      4: "Rental bicycle",
                      6: "Household vehicle 1",
                      7: "Household vehicle 2",
                      8: "Household vehicle 3",
                      9: "Household vehicle 4",
                      10: "Household vehicle 5",
                      11: "Household vehicle 6",
                      12: "Household vehicle 7",
                      16: "Other household vehicle",
                      17: "Rental car",
                      18: "Carshare",
                      21: "Vanpool",
                      22: "Other auto",
                      23: "Bus",
                      24: "School bus",
                      25: "Intercity bus",
                      26: "Shuttle bus",
                      27: "Paratransit",
                      28: "Other bus",
                      30: "Subway",
                      31: "Airplane",
                      32: "Ferry or water taxi",
                      33: "Work car",
                      34: "Friends car",
                      36: "Taxi - Regular",
                      37: "Taxi - Rideshare",
                      38: "University bus or shuttle",
                      39: "Rail - Light",
                      41: "Rail - Intercity",
                      42: "Rail - Other",
                      43: "Skateboard",
                      44: "Golf cart",
                      45: "ATV",
                      47: "Other household motorcycle",
                      55: "Express bus/Rapid",
                      97: "Other mode",
                      150: "San Diego Coaster Line",
                      -9999: "Technical error",
                      -9998: "Participant non-response",
                      pd.NA: "Not Applicable"},
            "mode4": {1: "Walk/jog/wheelchair",
                      2: "Personal bicycle",
                      3: "Borrowed bicycle",
                      4: "Rental bicycle",
                      6: "Household vehicle 1",
                      7: "Household vehicle 2",
                      8: "Household vehicle 3",
                      9: "Household vehicle 4",
                      10: "Household vehicle 5",
                      11: "Household vehicle 6",
                      12: "Household vehicle 7",
                      16: "Other household vehicle",
                      17: "Rental car",
                      18: "Carshare",
                      21: "Vanpool",
                      22: "Other auto",
                      23: "Bus",
                      24: "School bus",
                      25: "Intercity bus",
                      26: "Shuttle bus",
                      27: "Paratransit",
                      28: "Other bus",
                      30: "Subway",
                      31: "Airplane",
                      32: "Ferry or water taxi",
                      33: "Work car",
                      34: "Friends car",
                      36: "Taxi - Regular",
                      37: "Taxi - Rideshare",
                      38: "University bus or shuttle",
                      39: "Rail - Light",
                      41: "Rail - Intercity",
                      42: "Rail - Other",
                      43: "Skateboard",
                      44: "Golf cart",
                      45: "ATV",
                      47: "Other household motorcycle",
                      55: "Express bus/Rapid",
                      97: "Other mode",
                      150: "San Diego Coaster Line",
                      -9999: "Technical error",
                      -9998: "Participant non-response",
                      pd.NA: "Not Applicable"},
            "transit_access": {1: "Walked or jogged",
                               2: "Rode a bike",
                               3: "Drove and parked a car",
                               4: "Got dropped off",
                               5: "Took a taxi",
                               6: "Transferred from other transit",
                               7: "Was already at the stop",
                               97: "Other",
                               99: "Not Applicable",
                               pd.NA: "Missing"},
            "transit_egress": {1: "Walked or jogged",
                               2: "Rode a bike",
                               3: "Drove and parked a car",
                               4: "Got dropped off",
                               5: "Took a taxi",
                               6: "Transferred from other transit",
                               7: "Was already at the stop",
                               97: "Other",
                               99: "Not Applicable",
                               pd.NA: "Missing"},
            "google_mode": {"DRIVE": "DRIVE",
                            "TRANSIT": "TRANSIT",
                            "WALK/BIKE": "WALK/BIKE",
                            np.NaN: "Not Applicable"},
            "driver": {1: "Driver",
                       2: "Passenger",
                       3: "Both (switched drivers during trip)",
                       pd.NA: "Missing"},
            "toll_no": {1: "No",  # flip mapping
                        0: "Yes",  # flip mapping
                        99: "Not Applicable",
                        pd.NA: "Missing"},
            "toll_express": {0: "No",
                             1: "Yes",
                             99: "Not Applicable",
                             pd.NA: "Missing"},
            "parklocation": {1: "My own driveway/garage",
                             2: "Someone elses driveway",
                             3: "Parking lot/garage",
                             4: "On street parking",
                             5: "Park & Ride lot",
                             6: "Did not park (e.g., waited, drop-off, drive-thru)",
                             97: "Other",
                             -9999: "Technical error",
                             -9998: "Participant non-response",
                             pd.NA: "Missing"},
            "parktype": {1: "Free parking (no cost)",
                         2: "Used a parking pass (any type)",
                         3: "Paid via cash, credit card, or ticket(s)",
                         4: "Reserved parking service (e.g., ParkingPanda)",
                         5: "Another person paid",
                         97: "Other",
                         -9999: "Technical error",
                         -9998: "Participant non-response",
                         pd.NA: "Missing"},
            "park_cost_dk": {0: "No",
                             1: "Yes",
                             pd.NA: "Missing"},
            "taxitype": {1: "I paid the fare myself (no reimbursement)",
                         2: "Employer paid (I am reimbursed)",
                         3: "Split/shared fare with other(s)",
                         4: "Someone else paid 100% (all of taxi fare)",
                         97: "Other",
                         -9999: "Technical error",
                         -9998: "Participant non-response",
                         pd.NA: "Missing"},
            "taxi_cost_dk": {0: "No",
                             1: "Yes",
                             pd.NA: "Missing"},
            "airtype": {1: "Personally paid the airfare cost",
                        2: "Employer paid 100%",
                        3: "Used miles/points to purchase flight",
                        4: "Someone else paid 100% (all of airfare cost)",
                        97: "Other",
                        -9999: "Technical error",
                        -9998: "Participant non-response",
                        pd.NA: "Missing"},
            "airfare_cost_dk": {0: "No",
                                1: "Yes",
                                pd.NA: "Missing"},
            "bustype": {-9999: "Technical error",
                        -9998: "Participant non-response",
                        1: "Free (no cost)",
                        2: "Used pass (any type)",
                        3: "Cash, credit card, or ticket(s)",
                        97: "Other",
                        98: "Do not know",
                        pd.NA: "Missing"},
            "bus_cost_dk": {0: "No",
                            1: "Yes",
                            pd.NA: "Missing"},
            "railtype": {1: "Free (no cost)",
                         2: "Used pass (any type)",
                         3: "Cash, credit card, or ticket(s)",
                         97: "Other",
                         98: "Do not know",
                         -9999: "Technical error",
                         -9998: "Participant non-response",
                         pd.NA: "Missing"},
            "rail_cost_dk": {0: "No",
                             1: "Yes",
                             pd.NA: "Missing"},
            "ferrytype": {1: "Free (no cost)",
                          2: "Used pass (any type)",
                          3: "Cash, credit card, or ticket(s)",
                          97: "Other",
                          98: "Do not know",
                          pd.NA: "Missing"},
            "ferry_cost_dk": {0: "No",
                              1: "Yes",
                              pd.NA: "Missing"},
            "parkride_lot": {1: "Lot #16 Poway Rd at Sabre Springs Pkwy",
                             2: "Lot #17 I-8 at Taylor St",
                             3: "Lot #20 I-805 at Governor Dr",
                             4: "Lot #24 I-805 at Mira Mesa Blvd & Vista Sorrento Pkwy",
                             5: "Lot #26 Carmel Mountain Rd at Rancho Carmel Dr",
                             6: "Lot #31 SR 56 at Rancho Carmel Dr",
                             7: "Lot #4 Carmel Mountain Rd at Freeport Rd",
                             8: "Lot #43 I-5 at Gilman Dr",
                             9: "Lot #51 I-15 at Rancho Penasquitos Blvd",
                             10: "Lot #53 Carmel Mountain Rd at Paseo Cardiel",
                             11: "Lot #57 Carmel Mountain Rd at Stoney Creek Rd",
                             12: "Lot #6 I-15 at Mira Mesa Blvd",
                             13: "Lot #65 I-15 at Rancho Bernardo Rd",
                             14: "Lot #7 I-5 at Carmel Valley Rd & Sorrento Valley Rd",
                             15: "Lot #76 I-15 at Scripps Poway Pkwy",
                             16: "Lot #78 I-805 at Childrens Way",
                             17: "Lot #80 Caliente Ave",
                             18: "Lot #1 SR 94 at Sweetwater Springs Blvd",
                             19: "Lot #28 SR 94 at Potrero Post Office",
                             20: "Lot #33 I-15 at Deer Springs Rd",
                             21: "Lot #34 I-15 at Mountain Meadow Rd",
                             22: "Lot #35 I-15 at Gopher Canyon Rd",
                             23: "Lot #37 SR 94 at Avocado Blvd",
                             24: "Lot #40 SR 54 at Jamacha Blvd",
                             25: "Lot #71 Sweetwater Springs Blvd at Austin Dr",
                             26: "Lot #11 SR 78 at Broadway",
                             27: "Lot #3 Felicita Ave at Escondido Blvd",
                             28: "Lot #30 I-15 at El Norte Pkwy",
                             29: "Lot #38 7 Oakes Rd at El Norte Pkwy",
                             30: "Lot #81 Westfield North County",
                             31: "Lot #22 I-8 at Murray Dr",
                             32: "Lot #59 Bancroft Dr at Grossmont Blvd",
                             33: "Lot #60 Severin Dr at Bancroft Dr",
                             34: "Lot #61 Severin Dr at Murray Dr",
                             35: "Lot #8 Lemon Grove Ave at High St",
                             36: "Lot #39 SR 78 at College Blvd (South)",
                             37: "Lot #44 I-5 at SR 78 & Moreno St",
                             38: "Lot #45 SR 78 at College Blvd (North)",
                             39: "Lot #5 Maxson St at Barnes St",
                             40: "Lot #73 Mission Ave at Frontier Dr",
                             41: "Lot #32 I-5 at La Costa Ave",
                             42: "Lot #47 I-5 at Birmingham Dr",
                             43: "Lot #62 Encinitas Blvd at Calle Magdelena",
                             44: "Lot #10 SR 67 at Mapleview St",
                             45: "Lot #2 SR 67 at Riverford Rd & Woodside Ave",
                             46: "Lot #42 I-8 at Lake Jennings Park Rd",
                             47: "Lot #48 Twin Peaks Rd at Budwin Ln",
                             48: "Lot #77 SR 67 at Poway Rd",
                             49: "Lot #25 SR 54 at Washington Ave",
                             50: "Lot #41 I-8 at Los Coches Rd",
                             51: "Lot #63 SR 67 at Day St",
                             52: "Lot #75 SR 67 at Dye Rd",
                             53: "Lot #70 Mission Gorge Rd at Big Rock Dr",
                             54: "Lot #72 North Magnolia Ave at Alexander Way",
                             55: "Lot #50 Telegraph Canyon Rd at Paseo Del Ray",
                             56: "Lot #56 East H St at Buena Vista Way",
                             57: "Lot #46 SR 76 at Sweetgrass Ln",
                             58: "Lot #29 I-8 at Japatul Valley Rd",
                             59: "Lot #69 SR 78 at Barham Dr",
                             60: "Lot #19 I-15 at SR 76",
                             61: "Lot #21 SR 78 at Sunset Dr & Seaview Pl",
                             62: "Lot #12 Lemon Grove Ave at Lincoln St",
                             63: "Lot #9 I-805 at Sweetwater Rd",
                             pd.NA: "Not Applicable"},
            "parkride_city": {1: "Bonsall",
                              2: "Chula Vista",
                              3: "Descanso",
                              4: "El Cajon",
                              5: "Encinitas",
                              6: "Escondido",
                              7: "La Mesa",
                              8: "Lakeside",
                              9: "Lemon Grove",
                              10: "National City",
                              11: "Oceanside",
                              12: "Pala",
                              13: "Poway",
                              14: "Ramona",
                              15: "San Diego",
                              16: "San Diego County",
                              17: "San Marcos",
                              18: "Santee",
                              19: "Vista",
                              pd.NA: "Not Applicable"}
        }

        for field in mappings:
            # define using pd.Categorical to maintain defined category order
            # without setting ordered parameter to True
            df[field] = pd.Categorical(
                df[field].map(mappings[field]),
                categories=mappings[field].values()
            )

            # add Not Applicable category if it does not already exist
            # this is added here for upcoming manual recode operations
            if "Not Applicable" not in df[field].cat.categories:
                df[field].cat.add_categories(["Not Applicable"], inplace=True)

        # set initial Missings for non-categorical variables
        # that need initial Missing values set
        missing_cols = [
            "revised_count",
            "origin_name",
            "destination_name",
            "origin_address",
            "destination_address",
            "o_purpose_other",
            "d_purpose_other"
        ]

        df[missing_cols] = df[missing_cols].fillna("Missing")

        # manual recodes for online (non-rMoves) only variables
        df.loc[df.data_source == "rMove",
               ["origin_name",
                "origin_address",
                "destination_name",
                "destination_address",
                "toll_no",
                "toll_noexpress",
                "toll_express",
                "transit_access",
                "transit_egress",
                "parkride_lot",
                "parkride_city"]] = "Not Applicable"

        # manual recodes for rMoves only variables
        # keep location_trip as blank (later SQL code to Not Applicable record)
        df.loc[df.data_source == "Online",
               ["revised_count",
                "error",
                "flag_teleport",
                "copied_trip",
                "analyst_merged",
                "analyst_split",
                "user_merged",
                "user_split",
                "added_trip",
                "nonproxy_derived_trip",
                "proxy_added_trip",
                "o_purpose_inferred",
                "d_purpose_inferred"]] = "Not Applicable"

        # Online-only surveys get NULL record for location_tripid
        df.loc[df.data_source == "Online", "location_tripid"] = int(0)

        # set origin/destination specify other purpose to Not Applicable
        # if origin/destination purpose not set to Other
        df.loc[~df.o_purpose.isin(["Other", "Other purpose"]),
               "o_purpose_other"] = "Not Applicable"
        df.loc[~df.d_purpose.isin(["Other", "Other purpose"]),
               "d_purpose_other"] = "Not Applicable"

        # manual recodes for transit access/egress mode variables
        transit_modes = ["Vanpool"
                         "Bus",
                         "School bus",
                         "Intercity bus",
                         "Shuttle bus",
                         "Paratransit",
                         "Other bus",
                         "Subway",
                         "Ferry or water taxi",
                         "University bus or shuttle",
                         "Rail - Light",
                         "Rail - Intercity",
                         "Rail - Other",
                         "Express bus/Rapid",
                         "San Diego Coaster Line"]

        df.loc[(~df.mode1.isin(transit_modes)) &
               (~df.mode2.isin(transit_modes)) &
               (~df.mode3.isin(transit_modes)) &
               (~df.mode4.isin(transit_modes)),
               ["transit_access",
                "transit_egress"]] = "Not Applicable"

        # manual recodes for auto trip only variables
        auto_modes = ["Household vehicle 1",
                      "Household vehicle 2",
                      "Household vehicle 3",
                      "Household vehicle 4",
                      "Household vehicle 5",
                      "Household vehicle 6",
                      "Household vehicle 7",
                      "Other household vehicle",
                      "Rental car",
                      "Carshare",
                      "Other auto",
                      "Work car",
                      "Friends car",
                      "Taxi - Regular",
                      "Taxi - Rideshare"]

        df.loc[(~df.mode1.isin(auto_modes)) &
               (~df.mode2.isin(auto_modes)) &
               (~df.mode3.isin(auto_modes)) &
               (~df.mode4.isin(auto_modes)),
               ["toll_no",
                "toll_noexpress",
                "toll_express"]] = "Not Applicable"

        auto_nontaxi_modes = ["Household vehicle 1",
                              "Household vehicle 2",
                              "Household vehicle 3",
                              "Household vehicle 4",
                              "Household vehicle 5",
                              "Household vehicle 6",
                              "Household vehicle 7",
                              "Other household vehicle",
                              "Rental car",
                              "Carshare",
                              "Other auto",
                              "Work car",
                              "Friends car"]

        df.loc[(~df.mode1.isin(auto_nontaxi_modes)) &
               (~df.mode2.isin(auto_nontaxi_modes)) &
               (~df.mode3.isin(auto_nontaxi_modes)) &
               (~df.mode4.isin(auto_nontaxi_modes)),
               ["driver",
                "parklocation"]] = "Not Applicable"

        # parking variable recodes
        df.loc[~df.parklocation.isin(["Someone elses driveway",
                                      "Parking lot/garage",
                                      "On street parking",
                                      "Park & Ride lot"]),
               "parktype"] = "Not Applicable"

        df.loc[~df.parktype.isin(["Paid via cash, credit card, or ticket(s)",
                                  "Reserved parking service (e.g., ParkingPanda)",
                                  "Another person paid"]),
               "park_cost_dk"] = "Not Applicable"

        df.loc[df.parklocation != "Park & Ride lot",
               ["parkride_lot",  "parkride_city"]] = "Not Applicable"

        # taxi variable recodes
        taxi_modes = ["Taxi - Regular", "Taxi - Rideshare"]

        df.loc[(~df.mode1.isin(taxi_modes)) &
               (~df.mode2.isin(taxi_modes)) &
               (~df.mode3.isin(taxi_modes)) &
               (~df.mode4.isin(taxi_modes)),
               "taxitype"] = "Not Applicable"

        df.loc[~df.taxitype.isin(["I paid the fare myself (no reimbursement)",
                                  "Employer paid (I am reimbursed)",
                                  "Split/shared fare with other(s)"]),
               "taxi_cost_dk"] = "Not Applicable"

        # airplane variable recodes
        df.loc[(df.mode1 != "Airplane") &
               (df.mode2 != "Airplane") &
               (df.mode3 != "Airplane") &
               (df.mode4 != "Airplane"),
               "airtype"] = "Not Applicable"

        df.loc[~df.taxitype.isin(["Personally paid the airfare cost",
                                  "Employer paid 100%"]),
               "airfare_cost_dk"] = "Not Applicable"

        # bus variable recodes
        bus_modes = ["Bus",
                     "Intercity bus",
                     "Express bus/Rapid"]

        df.loc[(~df.mode1.isin(bus_modes)) &
               (~df.mode2.isin(bus_modes)) &
               (~df.mode3.isin(bus_modes)) &
               (~df.mode4.isin(bus_modes)),
               "bustype"] = "Not Applicable"

        df.loc[df.bustype != "Cash, credit card, or ticket(s)",
               "bus_cost_dk"] = "Not Applicable"

        # rail variable recodes
        rail_modes = ["Bus",
                      "Intercity bus",
                      "Express bus/Rapid"]

        df.loc[(~df.mode1.isin(rail_modes)) &
               (~df.mode2.isin(rail_modes)) &
               (~df.mode3.isin(rail_modes)) &
               (~df.mode4.isin(rail_modes)),
               "railtype"] = "Not Applicable"

        df.loc[df.railtype != "Cash, credit card, or ticket(s)",
               "rail_cost_dk"] = "Not Applicable"

        # ferry variable recodes
        df.loc[(df.mode1 != "Ferry or water taxi") &
               (df.mode2 != "Ferry or water taxi") &
               (df.mode3 != "Ferry or water taxi") &
               (df.mode4 != "Ferry or water taxi"),
               "ferrytype"] = "Not Applicable"

        df.loc[df.ferrytype != "Cash, credit card, or ticket(s)",
               "ferry_cost_dk"] = "Not Applicable"

        # strip | characters from origin/destination address
        df["origin_address"] = df["origin_address"].str.replace("\|", "", regex=True)
        df["destination_address"] = df["destination_address"].str.replace("\|", "", regex=True)

        # create survey weight trip and weight person trip
        # use RSG weighting eligibility criteria
        # household must have one fully complete travel weekday
        # household travelers have one record per person per trip
        # non-household travelers have no such record
        df.loc[df.h_complete_weekdays >= 1, "weight_person_trip"] = 1

        # initialize trip weight at 1 for valid trips then weight
        # by householders on trip (accounts for non-response to question)
        df.loc[df.h_complete_weekdays >= 1, "weight_trip"] = 1
        condition = (df.h_complete_weekdays >= 1) & df.travelers_hh.isin(range(1, 11))
        df.loc[condition, "weight_trip"] = 1 / df.loc[condition, "travelers_hh"].to_numpy()

        # WKT origin point geometry from lat/long in EPSG:2230
        df["origin_shape"] = self.point_wkt(zip(df["origin_lng"], df["origin_lat"]), "EPSG:2230")

        # WKT destination point geometry from lat/long in EPSG:2230
        df["destination_shape"] = self.point_wkt(zip(df["destination_lng"], df["destination_lat"]), "EPSG:2230")

        # rename columns
        df.rename(columns={
            "tripid": "trip_id",
            "tripid_linked": "trip_id_linked",
            "location_tripid": "trip_id_location",
            "personid": "person_id",
            "hhid": "household_id",
            "traveldate": "travel_date",
            "daynum": "travel_day_number",
            "travel_dow": "travel_day_of_week",
            "svy_complete": "completed_trip_survey",
            "completed_at": "completed_date",
            "day_hhcomplete": "completed_household_survey",
            "pday_complete": "completed_person_survey",
            "h_complete_weekdays": "number_household_survey_weekdays",
            "analyst_merged": "analyst_merged_trip",
            "analyst_split": "analyst_split_trip",
            "user_merged": "user_merged_trip",
            "user_split": "user_split_trip",
            "origin_lat": "origin_latitude",
            "origin_lng": "origin_longitude",
            "destination_lat": "destination_latitude",
            "destination_lng": "destination_longitude",
            "o_purpose": "origin_purpose",
            "o_purpose_other": "origin_purpose_other_specify",
            "o_purpose_inferred": "origin_purpose_inferred",
            "d_purpose": "destination_purpose",
            "d_purpose_other": "destination_purpose_other_specify",
            "d_purpose_inferred": "destination_purpose_inferred",
            "travelers_total": "travelers",
            "travelers_hh": "travelers_household",
            "travelers_nonhh": "travelers_non_household",
            "mode1": "mode_1",
            "mode2": "mode_2",
            "mode3": "mode_3",
            "mode4": "mode_4",
            "transit_access": "mode_transit_access",
            "transit_egress": "mode_transit_egress",
            "toll_no": "toll_road",
            "toll_express": "toll_road_express",
            "parklocation": "parking_location",
            "parktype": "parking_pay_type",
            "parkcost": "parking_cost",
            "park_cost_dk": "parking_cost_dk",
            "parkegress_time": "parking_egress_duration",
            "taxitype": "taxi_pay_type",
            "taxicost": "taxi_cost",
            "airtype": "airplane_pay_type",
            "airfarecost": "airplane_cost",
            "airfare_cost_dk": "airplane_cost_dk",
            "bustype": "bus_pay_type",
            "buscost": "bus_cost",
            "railtype": "rail_pay_type",
            "railcost": "rail_cost",
            "ferrytype": "ferry_pay_type",
            "ferrycost": "ferry_cost",
            "trip_path_distance": "distance",
            "trip_duration": "duration",
            "trip_duration_reported": "duration_reported",
            "speed_mph": "speed",
            "h_multiday_factor": "weight_household_multiday_factor",
            "multiday_weight_456x": "weight_person_multiday_456x"
        },
            inplace=True)

        return df[["trip_id",
                   "trip_id_linked",
                   "trip_id_location",
                   "person_id",
                   "household_id",
                   "travel_date",
                   "travel_day_number",
                   "travel_day_of_week",
                   "data_source",
                   "completed_trip_survey",
                   "completed_date",
                   "completed_household_survey",
                   "completed_person_survey",
                   "number_household_survey_weekdays",
                   "revised_at",
                   "revised_count",
                   "error",
                   "flag_teleport",
                   "copied_trip",
                   "analyst_merged_trip",
                   "analyst_split_trip",
                   "user_merged_trip",
                   "user_split_trip",
                   "added_trip",
                   "nonproxy_derived_trip",
                   "proxy_added_trip",
                   "unlinked_transit_trip",
                   "origin_name",
                   "origin_address",
                   "origin_latitude",
                   "origin_longitude",
                   "origin_shape",
                   "destination_name",
                   "destination_address",
                   "destination_latitude",
                   "destination_longitude",
                   "destination_shape",
                   "origin_purpose",
                   "origin_purpose_other_specify",
                   "origin_purpose_inferred",
                   "destination_purpose",
                   "destination_purpose_other_specify",
                   "destination_purpose_inferred",
                   "departure_time",
                   "arrival_time",
                   "travelers",
                   "travelers_household",
                   "travelers_non_household",
                   "mode_1",
                   "mode_2",
                   "mode_3",
                   "mode_4",
                   "mode_transit_access",
                   "mode_transit_egress",
                   "google_mode",
                   "driver",
                   "toll_road",
                   "toll_road_express",
                   "parking_location",
                   "parking_pay_type",
                   "parking_cost",
                   "parking_cost_dk",
                   "parking_egress_duration",
                   "taxi_pay_type",
                   "taxi_cost",
                   "taxi_cost_dk",
                   "airplane_pay_type",
                   "airplane_cost",
                   "airplane_cost_dk",
                   "bus_pay_type",
                   "bus_cost",
                   "bus_cost_dk",
                   "rail_pay_type",
                   "rail_cost",
                   "rail_cost_dk",
                   "ferry_pay_type",
                   "ferry_cost",
                   "ferry_cost_dk",
                   "parkride_lot",
                   "parkride_city",
                   "distance",
                   "duration",
                   "duration_reported",
                   "speed",
                   "weight_trip",
                   "weight_person_trip",
                   "weight_household_multiday_factor",
                   "weight_person_multiday_456x"]]

    @property
    @lru_cache(maxsize=1)
    def vehicles(self) -> pd.DataFrame:
        """ Household vehicle list containing the following columns:
                vehicle_id - unique identifier of vehicle
                household_id - unique identifier of household
                vehicle_number - unique identifier of vehicle within household
                year - vehicle year
                make - vehicle make
                model - vehicle model
                fuel_type - vehicle fuel type (Hybrid, Electric, Diesel, Gas,
                    Other, Flex Fuel)
                how_obtained - how was vehicle obtained (Own, Lease, Other,
                    Employer/Institutional Car)
                toll_transponder - vehicle has toll transponder
                residence_parking_pass - vehicle has residence parking pass
                    (No pass needed - typically park on street,
                    No pass needed - typically park at residence,
                    No pass needed - typically park elsewhere,
                    Yes, vehicle has permit for parking at/near residence)
                residence_parking_cost_unknown - residence monthly parking
                    cost exists but is unknown to surveyed respondent
                residence_parking_cost - residence monthly parking cost """

        # combine HHTBS and AT data-sets
        df = pd.concat(
            [
                pd.read_csv(
                    "../data/sdrts/SDRTS_Vehicle_Data_20170731.csv",
                    usecols=["hhid",
                             "vehnum",
                             "year",
                             "make",
                             "model",
                             "fuel",
                             "obtain",
                             "tolltransp",
                             "respark_pass",
                             "respark_pass_monthly_cost",
                             "respark_pass_dontknow_cost"],
                    dtype={"fuel": "Int8",
                           "obtain": "Int8",
                           "tolltransp": "Int8",
                           "respark_pass": "Int8",
                           "respark_pass_dontknow_cost": "Int8"}
                ),
                pd.read_csv(
                    "../data/at/SDRTS_AT_Vehicle_Data_20170809.csv",
                    usecols=["hhid",
                             "vehnum",
                             "year",
                             "make",
                             "model",
                             "fuel",
                             "obtain",
                             "tolltransp",
                             "respark_pass",
                             "respark_pass_monthly_cost",
                             "respark_pass_dontknow_cost"],
                    dtype={"fuel": "Int8",
                           "obtain": "Int8",
                           "tolltransp": "Int8",
                           "respark_pass": "Int8",
                           "respark_pass_dontknow_cost": "Int8"}
                )
            ],
            ignore_index=True
        )

        # apply exhaustive field mappings where applicable
        mappings = {
            "fuel": {1: "Gas",
                     2: "Diesel",
                     3: "Hybrid",
                     4: "Electric",
                     5: "Flex Fuel",
                     97: "Other"},
            "obtain": {1: "Own",
                       2: "Lease",
                       3: "Employer/Institutional Car",
                       97: "Other"},
            "tolltransp": {1: "No",
                           2: "Yes"},
            "respark_pass": {1: "Yes, vehicle has permit for parking at/near residence",
                             2: "No pass needed - typically park at residence",
                             3: "No pass needed - typically park on street",
                             4: "No pass needed - typically park elsewhere"},
            "respark_pass_dontknow_cost": {0: "No",
                                           1: "Yes",
                                           pd.NA: "Not Applicable"}
        }

        for field in mappings:
            # define using pd.Categorical to maintain defined category order
            # without setting ordered parameter to True
            df[field] = pd.Categorical(
                df[field].map(mappings[field]),
                categories=mappings[field].values()
            )

        # create vehicle surrogate key
        df["vehicle_id"] = df.groupby(["hhid", "vehnum"]).ngroup() + 1

        # rename columns
        df.rename(columns={
            "hhid": "household_id",
            "vehnum": "vehicle_number",
            "fuel": "fuel_type",
            "obtain": "how_obtained",
            "tolltransp": "toll_transponder",
            "respark_pass": "residence_parking_pass",
            "respark_pass_monthly_cost": "residence_parking_monthly_cost",
            "respark_pass_dontknow_cost": "residence_parking_cost_unknown"},
            inplace=True)

        return df[["vehicle_id",
                   "household_id",
                   "vehicle_number",
                   "year",
                   "make",
                   "model",
                   "fuel_type",
                   "how_obtained",
                   "toll_transponder",
                   "residence_parking_pass",
                   "residence_parking_cost_unknown",
                   "residence_parking_monthly_cost"]]
