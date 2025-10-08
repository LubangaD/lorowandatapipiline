# Quality Control (QC) rules for Afrisense Weather Data
# Level-1 checks translated from notebook to Spark

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DoubleType, TimestampType


# -----------------------------
# Preprocessing + time-gap QC rules
# -----------------------------
# Busia-specific thresholds
thr = {
    'Tair_min_phy': 10.0, 'Tair_max_phy': 45.0,
    'Tair_warn_min_clim': 12.0, 'Tair_warn_max_clim': 36.0,
    'Tair_max_step_warn': 3.0, 'Tair_max_step_fail': 5.0,
    'RH_min_phy': 0.0, 'RH_max_phy': 100.0, 'RH_warn_min_clim': 20.0,
    'Rain_min_inc': 0.0, 'Rain_warn_15min': 20.0, 'Rain_max_15min': 40.0,
    'Rain_warn_daily': 150.0, 'Rain_max_daily': 300.0,
    'Wind_min_phy': 0.0, 'Wind_warn_max_clim': 20.0, 'Wind_max_phy': 40.0,
    'WindDir_min_phy': 0.0, 'WindDir_max_phy': 360.0, 'WindDir_requires_wind': 0.3,
    'P_min_phy': 750.0, 'P_max_phy': 1010.0, 'P_warn_abs_dev': 25.0, 'P_fail_abs_dev': 40.0,
    'Min_daily_avail': 0.8
}


def qc_encode_col(cond_ok, cond_warn=None, cond_fail=None):
    """
    Return a Column expression that follows the notebook encoding:
      -2 = FAIL, 0 = WARNING, 1 = OK
    Precedence: FAIL > WARNING > OK
    If none match the default is 1 (mimics notebook behavior).
    """
    # QC function with encoding scheme: 1=OK, 0=Warning, -2=Fail
    inner_ok = F.when(cond_ok, F.lit(1)).otherwise(F.lit(1))

    if cond_warn is not None:
        inner_warn = F.when(cond_warn, F.lit(0)).otherwise(inner_ok)
    else:
        inner_warn = inner_ok

    if cond_fail is not None:
        expr = F.when(cond_fail, F.lit(-2)).otherwise(inner_warn)
    else:
        expr = inner_warn

    return expr


def parse_and_dedup(df: DataFrame, ts_col: str = "processing_timestamp") -> DataFrame:
    """
    Parse timestamp column (if string) and drop exact duplicates on device_id+timestamp.
    Returns DF with a proper timestamp column.
    """

    # Try to parse timestamp to TimestampType (works when already TimestampType too)
    df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
    # Drop duplicates by station + timestamp (same as pandas drop_duplicates)
    df = df.dropDuplicates(["device_id", ts_col])
    return df


def compute_time_diff(df: DataFrame, ts_col: str = "processing_timestamp") -> DataFrame:
    """
    Compute time difference (in minutes) between consecutive rows per device_id.
    Adds a 'time_diff' column (float minutes).
    """
    w = Window.partitionBy("device_id").orderBy(ts_col)
    # unix_timestamp returns seconds since epoch; cast timestamp to long then diff / 60
    df = df.withColumn("_ts_seconds", F.col(ts_col).cast("long"))
    df = df.withColumn("_prev_ts_seconds", F.lag("_ts_seconds").over(w))
    df = df.withColumn("time_diff", (F.col("_ts_seconds") - F.col("_prev_ts_seconds")) / 60.0)
    df = df.drop("_ts_seconds", "_prev_ts_seconds")
    return df


def qc_time_gap(df: DataFrame, min_gap: float = 14.0, max_gap: float = 16.0) -> DataFrame:
    """
    Binary flag for time gap:
      QC_time_gap = 0 -> OK (between min_gap and max_gap inclusive)
      QC_time_gap = -1 -> irregular (outside range)
    Note: NA time_diff stays null (QC_time_gap will be 0 by default).
    """
    # default 0, set -1 where outside range and not null
    cond_irregular = (~F.col("time_diff").between(min_gap, max_gap)) & F.col("time_diff").isNotNull()
    df = df.withColumn("QC_time_gap", F.when(cond_irregular, F.lit(-1)).otherwise(F.lit(0)))
    return df


# Atmospheric Pressure
def convert_pressure_hpa(df: DataFrame, pressure_col: str = "barometric_pressure") -> DataFrame:
    """
    Convert pressure to hPa:
     - If median pressure > 2000 assume input is Pa and divide by 100.
     - Otherwise keep as-is (already hPa).
    We compute an *approximate* median using approxQuantile (fast for big data).
    Adds/returns column 'pressure_hpa' (double).
    """
    # compute approximate median with relative error 1%
    try:
        median = df.approxQuantile(pressure_col, [0.5], 0.01)[0]
    except Exception:
        median = None

    if median is not None and median > 2000:
        df = df.withColumn("pressure_hpa", (F.col(pressure_col).cast(DoubleType()) / 100.0))
    else:
        df = df.withColumn("pressure_hpa", F.col(pressure_col).cast(DoubleType()))
    return df


# def compute_expected_pressure(df: DataFrame, altitude_col: str = "altitude") -> DataFrame:
#     """
#     Compute expected pressure from altitude and absolute deviation:
#       p_expected_hpa = 1013.25 * exp(-altitude / 8434.0)
#       pressure_abs_dev_hpa = abs(pressure_hpa - p_expected_hpa)
#     """
#     df = df.withColumn(altitude_col, F.coalesce(F.col(altitude_col).cast(DoubleType()), F.lit(0.0)))
#     df = df.withColumn("p_expected_hpa", 1013.25 * F.exp(-F.col(altitude_col) / F.lit(8434.0)))
#     df = df.withColumn("pressure_abs_dev_hpa", F.abs(F.col("pressure_hpa") - F.col("p_expected_hpa")))
#     return df


def clean_negative_rain(df: DataFrame, rain_col: str = "rain_gauge") -> DataFrame:
    """
    Replace negative rain increments with 0.0 (not a QC flag).
    """
    df = df.withColumn(rain_col, F.when(F.col(rain_col) < 0, F.lit(0.0)).otherwise(F.col(rain_col)))
    return df


# -----------------------------
# Range & Step QC Functions
# -----------------------------


# --- Temperature QC ---
def qc_temperature_range(df: DataFrame, thr: dict) -> DataFrame:
    df = df.withColumn(
        "QC_Tair_range",
        qc_encode_col(
            cond_ok=(F.col("air_temperature").between(thr['Tair_warn_min_clim'], thr['Tair_warn_max_clim'])),
            cond_warn=((F.col("air_temperature") >= thr['Tair_min_phy']) & (F.col("air_temperature") <= thr['Tair_max_phy'])),
            cond_fail=((F.col("air_temperature") < thr['Tair_min_phy']) | (F.col("air_temperature") > thr['Tair_max_phy']))
        )
    )
    return df


# --- Relative Humidity QC ---
def qc_rh_range(df: DataFrame, thr: dict) -> DataFrame:
    df = df.withColumn(
        "QC_RH_range",
        qc_encode_col(
            cond_ok=(F.col("air_humidity").between(thr['RH_warn_min_clim'], thr['RH_max_phy'])),
            cond_warn=(F.col("air_humidity").between(thr['RH_min_phy'], thr['RH_max_phy'])),
            cond_fail=((F.col("air_humidity") < thr['RH_min_phy']) | (F.col("air_humidity") > thr['RH_max_phy']))
        )
    )
    return df


# --- Wind Speed QC ---
def qc_windspeed_range(df: DataFrame, thr: dict) -> DataFrame:
    df = df.withColumn(
        "QC_WindSpeed_range",
        qc_encode_col(
            cond_ok=(F.col("wind_speed").between(thr['Wind_min_phy'], thr['Wind_warn_max_clim'])),
            cond_warn=(F.col("wind_speed").between(thr['Wind_min_phy'], thr['Wind_max_phy'])),
            cond_fail=((F.col("wind_speed") < thr['Wind_min_phy']) | (F.col("wind_speed") > thr['Wind_max_phy']))
        )
    )
    return df


# --- Wind Direction QC ---
def qc_winddir_range(df: DataFrame, thr: dict) -> DataFrame:
    df = df.withColumn(
        "QC_WindDir_range",
        qc_encode_col(
            cond_ok=(F.col("wind_direction_sensor").between(thr['WindDir_min_phy'], thr['WindDir_max_phy'])),
            cond_fail=((F.col("wind_direction_sensor") < thr['WindDir_min_phy']) | (F.col("wind_direction_sensor") > thr['WindDir_max_phy']))
        )
    )
    return df


def qc_winddir_requires_wind(df: DataFrame, thr: dict) -> DataFrame:
    """
    If wind_speed < WindDir_requires_wind → QC_WindDir_requires_wind = 0
    Else 1
    """
    df = df.withColumn(
        "QC_WindDir_requires_wind",
        F.when(F.col("wind_speed") < thr['WindDir_requires_wind'], F.lit(0)).otherwise(F.lit(1))
    )
    return df


# --- Pressure QC ---
# def qc_pressure_range(df: DataFrame, thr: dict) -> DataFrame:
#     df = df.withColumn(
#         "QC_Pressure_range",
#         qc_encode_col(
#             cond_ok=(F.col("pressure_abs_dev_hpa") < thr['P_warn_abs_dev']),
#             cond_warn=(F.col("pressure_abs_dev_hpa") < thr['P_fail_abs_dev']),
#             cond_fail=(F.col("pressure_hpa") < thr['P_min_phy']) | (F.col("pressure_hpa") > thr['P_max_phy']) | (F.col("pressure_abs_dev_hpa") > thr['P_fail_abs_dev'])
#         )
#     )
#     return df


# --- Rain (15-min increment) QC ---
def qc_rain_15min(df: DataFrame, thr: dict) -> DataFrame:
    df = df.withColumn(
        "QC_Rain_15min",
        qc_encode_col(
            cond_ok=(F.col("rain_gauge") <= thr['Rain_warn_15min']),
            cond_warn=((F.col("rain_gauge") > thr['Rain_warn_15min']) & (F.col("rain_gauge") <= thr['Rain_max_15min'])),
            cond_fail=(F.col("rain_gauge") < thr['Rain_min_inc']) | (F.col("rain_gauge") > thr['Rain_max_15min'])
        )
    )
    return df


# --- Temperature Step QC ---
def compute_tair_step(df: DataFrame, ts_col: str = "processing_timestamp") -> DataFrame:
    """
    Compute difference between consecutive temperature readings per station.
    Adds 'tair_step' column.
    """
    w = Window.partitionBy("device_id").orderBy(ts_col)
    df = df.withColumn("tair_step", F.col("air_temperature") - F.lag("air_temperature").over(w))
    return df


def qc_tair_step(df: DataFrame, thr: dict) -> DataFrame:
    df = df.withColumn(
        "QC_Tair_step",
        qc_encode_col(
            cond_ok=(F.abs(F.col("tair_step")) <= thr['Tair_max_step_warn']),
            cond_warn=(F.abs(F.col("tair_step")) <= thr['Tair_max_step_fail']),
            cond_fail=(F.abs(F.col("tair_step")) > thr['Tair_max_step_fail'])
        )
    )
    return df


# --- Daily Aggregations ---
def compute_daily_metrics(df: DataFrame, thr: dict, ts_col: str = "processing_timestamp", date_col: str = "date") -> DataFrame:
    """
    Compute daily availability fraction and daily rain totals.
    Adds: date_local, daily_valid_frac, daily_rain_mm
    """
    #  aggregate per device & date
    daily_metrics = df.groupBy("device_id", date_col).agg(
        (F.count(ts_col) / F.lit(96.0)).alias("daily_valid_frac"),
        F.sum("rain_gauge").alias("daily_rain_mm")
    )
    
    df = df.join(daily_metrics, on=["device_id", date_col], how="left")
    return df


def qc_daily_availability(df: DataFrame, thr: dict) -> DataFrame:    
    df = df.withColumn(
        "QC_Daily_Availability",
        F.when(F.col("daily_valid_frac") >= thr['Min_daily_avail'], F.lit(1)).otherwise(F.lit(0))
    )
    return df


def qc_rain_daily(df: DataFrame, thr: dict) -> DataFrame:
    
    df = df.withColumn(
        "QC_Rain_Daily",
        qc_encode_col(
            cond_ok=(F.col("daily_rain_mm") <= thr['Rain_warn_daily']),
            cond_warn=(F.col("daily_rain_mm") <= thr['Rain_max_daily']),
            cond_fail=(F.col("daily_rain_mm") > thr['Rain_max_daily'])
        )
    )
    return df
def collect_qc_check_results(df: DataFrame) -> DataFrame:
    """
    Collect names of failed and warning QC checks into arrays.
    Returns df with qc_failed_checks and qc_warning_checks columns.
    """
    # Define all QC columns
    qc_columns = [
        "QC_time_gap",
        "QC_Tair_range",
        "QC_RH_range",
        "QC_WindSpeed_range",
        "QC_WindDir_range",
        "QC_WindDir_requires_wind",
        "QC_Rain_15min",
        "QC_Tair_step",
        "QC_Daily_Availability",
        "QC_Rain_Daily"
    ]
    
    # Build array of failed check names (where value = -2)
    failed_checks = F.array_compact(F.array(*[
        F.when(F.col(c) == -2, F.lit(c)).otherwise(F.lit(None))
        for c in qc_columns
    ]))
    
    # Build array of warning check names (where value = 0)
    warning_checks = F.array_compact(F.array(*[
        F.when(F.col(c) == 0, F.lit(c)).otherwise(F.lit(None))
        for c in qc_columns
    ]))
    
    df = df.withColumn("qc_failed_checks", failed_checks)
    df = df.withColumn("qc_warning_checks", warning_checks)
    
    return df

# def qc_check_level1(df: DataFrame, thr: dict) -> DataFrame:
#     """
#     Apply all Level-1 QC checks sequentially and return enriched dataframe.
#     """

#     # Preprocessing
#     df = parse_and_dedup(df)
#     df = compute_time_diff(df)
#     df = qc_time_gap(df)
#     df = clean_negative_rain(df)
#     df = convert_pressure_hpa(df)
#     # df = compute_expected_pressure(df)

#     # Range + step checks
#     df = qc_temperature_range(df, thr)
#     df = qc_rh_range(df, thr)
#     df = qc_windspeed_range(df, thr)
#     df = qc_winddir_range(df, thr)
#     df = qc_winddir_requires_wind(df, thr)
#     # df = qc_pressure_range(df, thr)
#     df = qc_rain_15min(df, thr)

#     df = compute_tair_step(df)
#     df = qc_tair_step(df, thr)

#     # Daily checks
#     df = compute_daily_metrics(df, thr)
#     df = qc_daily_availability(df, thr)
#     df = qc_rain_daily(df, thr)

#     return df

def qc_check_level1(df: DataFrame, thr: dict) -> DataFrame:
    """
    Apply all Level-1 QC checks sequentially and return enriched dataframe.
    """

    # Preprocessing
    df = parse_and_dedup(df)
    df = compute_time_diff(df)
    df = qc_time_gap(df)
    df = clean_negative_rain(df)
    df = convert_pressure_hpa(df)

    # Range + step checks
    df = qc_temperature_range(df, thr)
    df = qc_rh_range(df, thr)
    df = qc_windspeed_range(df, thr)
    df = qc_winddir_range(df, thr)
    df = qc_winddir_requires_wind(df, thr)
    df = qc_rain_15min(df, thr)

    df = compute_tair_step(df)
    df = qc_tair_step(df, thr)

    # Daily checks
    df = compute_daily_metrics(df, thr)
    df = qc_daily_availability(df, thr)
    df = qc_rain_daily(df, thr)

    # Collect failed and warning check names into arrays
    df = collect_qc_check_results(df)

    return df