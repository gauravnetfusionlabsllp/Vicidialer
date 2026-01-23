
import pandas as pd
FILE_PATH = r"C:\Users\ADITYA\Downloads\Qcode - Part 2 Recon.xlsx"


FUND_SHEET = "Fund Accounting"
BANK1_SHEET = "Bank Statement 1"
BANK2_SHEET = "Bank Statement 2"

DATE_COL = "Date"
DESC_COL = "Description"
AMOUNT_COL = "Amount"

AMOUNT_TOLERANCE = 0.01
DATE_TOLERANCE_DAYS = 0

OUTPUT_FILE = "Reconciliation_Output.xlsx"

# ================= LOAD =================
fund = pd.read_excel(FILE_PATH, sheet_name=FUND_SHEET)
bank1 = pd.read_excel(FILE_PATH, sheet_name=BANK1_SHEET)
bank2 = pd.read_excel(FILE_PATH, sheet_name=BANK2_SHEET)

for df in [fund, bank1, bank2]:
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])
    df[AMOUNT_COL] = df[AMOUNT_COL].astype(float)
    df[DESC_COL] = df[DESC_COL].str.strip().str.lower()

bank = pd.concat([
    bank1.assign(Source="Bank1"),
    bank2.assign(Source="Bank2")
], ignore_index=True)

# ================= GROUP FUND =================
fund_grouped = (
    fund
    .groupby([DATE_COL, DESC_COL], as_index=False)
    .agg({
        AMOUNT_COL: "sum",
        "Amount": "count"
    })
    .rename(columns={
        AMOUNT_COL: "Fund_Total_Amount",
        "Amount": "Fund_Lines_Count"
    })
)

# ================= MATCHING =================
matched = []
used_fund_groups = set()
used_bank_rows = set()

for b_idx, b_row in bank.iterrows():
    candidates = fund_grouped[
        (abs(fund_grouped["Fund_Total_Amount"] - b_row[AMOUNT_COL]) <= AMOUNT_TOLERANCE) &
        (fund_grouped[DESC_COL] == b_row[DESC_COL]) &
        (abs((fund_grouped[DATE_COL] - b_row[DATE_COL]).dt.days) <= DATE_TOLERANCE_DAYS)
    ]

    if not candidates.empty:
        f_row = candidates.iloc[0]

        matched.append({
            "Bank_Date": b_row[DATE_COL],
            "Bank_Amount": b_row[AMOUNT_COL],
            "Bank_Source": b_row["Source"],
            "Description": b_row[DESC_COL],
            "Fund_Date": f_row[DATE_COL],
            "Fund_Total": f_row["Fund_Total_Amount"],
            "Fund_Lines": f_row["Fund_Lines_Count"]
        })

        used_fund_groups.add((f_row[DATE_COL], f_row[DESC_COL]))
        used_bank_rows.add(b_idx)

# ================= UNMATCHED =================
unmatched_fund = fund[
    ~fund.set_index([DATE_COL, DESC_COL]).index.isin(used_fund_groups)
]

unmatched_bank = bank.drop(index=used_bank_rows)

# ================= EXPORT =================
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    pd.DataFrame(matched).to_excel(writer, sheet_name="Matched_Distributed", index=False)
    unmatched_fund.to_excel(writer, sheet_name="Unmatched_Fund", index=False)
    unmatched_bank.to_excel(writer, sheet_name="Unmatched_Bank", index=False)

print("Distributed reconciliation completed.")


import pandas as pd

FILE_PATH = r"C:\Users\ADITYA\Downloads\Qcode - Part 2 Recon.xlsx"


xls = pd.ExcelFile(FILE_PATH)
print(xls.sheet_names)
