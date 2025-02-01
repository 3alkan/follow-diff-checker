import pandas as pd
from constants import *
from helpers import *
import duckdb

try:
    # Load JSON data into DataFrames
    frs = json2df(frs_name, "string_list_data")
    fws = json2df(fws_name, ['relationships_following', 'string_list_data'])
    pending = json2df(pending_name, ['relationships_follow_requests_sent', 'string_list_data'])

    # Connect to DuckDB
    con = duckdb.connect()

    # Register DataFrames as DuckDB tables
    con.register("frs", frs)
    con.register("fws", fws)

    # Find common users (both following & followers)
    q1 = "SELECT u1.* FROM frs AS u1 INNER JOIN fws AS u2 ON u1.value = u2.value;"
    common_users = con.query(q1).df()

    # Register common_users as a DuckDB table
    con.register("common_users", common_users)

    # Find users in frs but NOT in common_users
    q2 = "SELECT * FROM frs WHERE value NOT IN (SELECT value FROM common_users);"
    frs_diff = con.query(q2).df()

    # Find users in fws but NOT in common_users
    q3 = "SELECT * FROM fws WHERE value NOT IN (SELECT value FROM common_users);"
    fws_diff = con.query(q3).df()

    # Save original DataFrames
    saveDF_As_CSV(frs, frs_name)
    saveDF_As_CSV(fws, fws_name)
    saveDF_As_CSV(pending, pending_name)

    # Save results
    saveDF_As_CSV(frs_diff, frs_name + "_diff")
    saveDF_As_CSV(fws_diff, fws_name + "_diff")

    print("CSV files saved successfully.")

except Exception as e:
    print("Error:", e)

finally:
    con.close()