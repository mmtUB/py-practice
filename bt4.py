import pandas as pd
df = pd.read_csv("C:/Users/uyda/OneDrive/Uyen-test/py/test.csv")

df["Tags"] = df["Tags"].astype(str).str.strip()

#filter not dxc
not_dxc_condition = ~df["Tags"].str.contains("dxc", case=False)
not_dxp = df[not_dxc_condition]

#filter dxc checks
dxp_condition = df["Tags"].str.contains("dxc", case=False, na=False)
dxp_checks = df[dxp_condition]


#filter b2b checks
b2b_condition = not_dxp["Tags"].str.contains("b2b", case=False, na=True)
b2b_checks =  not_dxp[b2b_condition]

#filter internal checks
internal_checks = not_dxp[~b2b_condition] & not_dxp["Tags"] != "[]"

everweb_checks = ~internal_checks


b2b_checks.to_csv("b2b_checks.csv", index=False)
internal_checks.to_csv("internal_checks.csv", index=False)
dxp_checks.to_csv("dxp_checks.csv", index=False)
everweb_checks.to_csv("everweb_checks.csv", index=False)











# filter B2B checks
# b2b_condition = df["Tags"].str.contains("b2b", case=False, na=False)

# b2b_checks = df[b2b_condition]
# remaining = df[~b2b_condition]

# b2b_checks.to_csv("b2b_checks.csv", index=False)

# #filter internal checks 
# internal_condition = remaining["Tags"].str.contains("epi|opti|optiq|gateway|personalization|checkid|cmp|service") & ~remaining["Tags"].str.contains("dxc")

# internal_checks = remaining[internal_condition]
# dxp_checks = remaining[~internal_condition]

# internal_checks.to_csv("internal_checks.csv", index=False)
# dxp_checks.to_csv("dxp_checks.csv",index=False) 
