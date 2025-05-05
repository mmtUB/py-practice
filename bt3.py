import pandas as pd

df = pd.read_csv("test.csv")
df["Hostname"] = df["Hostname"].astype(str).str.strip()

#filtered duplicated checks
condition_dup = df.duplicated(subset=["Hostname"], keep=False)

df_dup = df[condition_dup]
df_remaining_after_dup = df[~condition_dup]

#filter test hostnames
condition_test = df_remaining_after_dup["Hostname"].str.contains("inte|int|integration|prep|prod|prod-|production|beta|alpha|stage|stg|staging|ade.|test|uat|dev.|qa|cms|preview|prd|portal|intranet", case=False, na=False, regex=True)

df_filtered_test = df_remaining_after_dup[condition_test]
df_prod_checks = df_remaining_after_dup[~condition_test]

#filtered paused checks
condition_paused = df_prod_checks["Status"].str.contains("paused", case =False, na=False)

df_filtered_paused = df_prod_checks[condition_paused]
df_remaining = df_prod_checks[~condition_paused]

#output to csv files
df_filtered_test.to_csv("test_checks.csv", index=False)
df_dup.to_csv("duplicated_checks.csv", index=False)
df_filtered_paused.to_csv("paused_checks.csv", index=False)
df_remaining.to_csv("remaining_checks.csv", index=False)
