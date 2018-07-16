from app import app
import pandas as pd
from datetime import timedelta

# Load and Clean Up Data Frame
df = pd.read_csv('/Users/bcraft/Documents/subscription_overview/app/OrderMrr.csv')


df = df.rename(index=str, columns={"OrderMrr.StartDate": "StartDate", "OrderMrr.EndDate": "EndDate",
                                   "OrderMrr.Value": "OrderMrrDelta", "OrderMrr.Type": "Type",
                                   "OrderMrr.GeneratedReason":
                                       "GeneratedReason", "Subscription.Name": "SubscriptionName",
                                   "SubscriptionVersionAmendment.Code": "AmendmentCode",
                                   "RatePlanCharge.ChargeNumber": "ChargeNumber",
                                   "RatePlanCharge.Name": "RatePlanChargeName", "OrderAction.Type": "OrderActionType",
                                   "Order.OrderNumber": "OrderNumber", "Account.Name": "AccountName",
                                   "Account.Currency":
                                       "AccountCurrency"})
df = df.drop(columns=["OrderMrr.CreatedDate"])

# Group Duplicates Together
# Create a df of duplicate rows
Duplicate_rows = df.drop(columns=["AmendmentCode", "RatePlanChargeName", "OrderActionType", "OrderNumber",
                                  "GeneratedReason", "SubscriptionName", "AccountName", "AccountCurrency"])

# Determine Duplicates
Duplicate_rows = Duplicate_rows.groupby(["ChargeNumber", "StartDate", "EndDate"]).agg({'Type': 'count',
                                                                                       'OrderMrrDelta': 'sum'})
Duplicate_rows = Duplicate_rows.rename(index=str, columns={"OrderMrrDelta": "DuplicateMrrTotal", "Type": "Duplicate"})
Duplicate_rows = Duplicate_rows[Duplicate_rows.Duplicate != 1]
Duplicate_rows["Duplicate"] = 1

# Join Duplicate table to main df
df = pd.merge(df, Duplicate_rows, how='left', on=['ChargeNumber', 'StartDate', 'EndDate'])
df = df.fillna(0)

# Determine which Duplicate row to keep
BeginningChargeNumber = 0

delete = []

for index, row in df.iterrows():
    if row['Duplicate'] == 1.0 and row['ChargeNumber'] != BeginningChargeNumber:
        delete.append(0)

        BeginningChargeNumber = row['ChargeNumber']
        continue
    elif row['Duplicate'] == 1.0 and row['ChargeNumber'] == BeginningChargeNumber:
        delete.append(1)
        continue
    else:
        delete.append(0)

df['delete'] = delete

# Replaced Group by Total and Delta and Remove Duplicates Identified
df.loc[df.Duplicate == 1.0, ['OrderMrrDelta']] = df["DuplicateMrrTotal"]
df = df[df.delete != 1]
df = df.drop(columns=["Duplicate", "delete", "DuplicateMrrTotal"])

# Create a df of the ChargeNumbers with more than one line
ChargeNums = df.groupby(["ChargeNumber"]).count().reset_index()
ChargeNums = ChargeNums[ChargeNums['StartDate'] > 1]
ChargeNums = ChargeNums["ChargeNumber"]
ChargeNums = ChargeNums.to_frame()
ChargeNums['MultiLine'] = 1

# Add the ChargeNums data frame to og df
df = pd.merge(df, ChargeNums, how='left', on='ChargeNumber')
df = df.fillna(0)
df = df.sort_values(by=["ChargeNumber", "StartDate"]).reset_index().drop(columns="index")

# Determine Max StartDate Per ChargeNumber and add to og df
MaxStartDate = df.groupby('ChargeNumber')['StartDate'].max()
MaxStartDate = MaxStartDate.to_frame()
MaxStartDate = MaxStartDate.rename(index=str, columns={"StartDate": "MaxStartDate"})
df = pd.merge(df, MaxStartDate, how='left', on='ChargeNumber')

# Create a ChargeOrder Column
ChargeOrder = []

BeginningChargeNumber = df["ChargeNumber"].iloc[0]
BeginningChargeOrder = 1

for index, row in df.iterrows():
    if row['MultiLine'] == 1 and row['ChargeNumber'] == BeginningChargeNumber:
        ChargeOrder.append(BeginningChargeOrder)
        BeginningChargeOrder = BeginningChargeOrder + 1
        continue
    elif row['MultiLine'] == 1 and row['ChargeNumber'] != BeginningChargeNumber:
        BeginningChargeNumber = row['ChargeNumber']
        BeginningChargeOrder = 1
        ChargeOrder.append(BeginningChargeOrder)
        BeginningChargeOrder = BeginningChargeOrder + 1
        continue
    else:
        ChargeOrder.append(1)

df['ChargeOrder'] = ChargeOrder

df = df.sort_values(by=["ChargeNumber", "StartDate"], ascending=False)

# Enforce DateTime
df["StartDate"] = pd.to_datetime(df["StartDate"])
df["MaxStartDate"] = pd.to_datetime(df["MaxStartDate"])
df["EndDate"] = pd.to_datetime(df["EndDate"])

# Create a ChargeEndDate Column
ChargeEndDate = []

BeginningChargeEndDate = df["StartDate"].iloc[0]

for index, row in df.iterrows():
    if row['StartDate'] == row['MaxStartDate']:
        ChargeEndDate.append(row["EndDate"])
        BeginningChargeEndDate = row["StartDate"]
        continue
    elif row['StartDate'] != row['MaxStartDate']:
        BeginningChargeEndDate = BeginningChargeEndDate - timedelta(days=1)
        ChargeEndDate.append(BeginningChargeEndDate)
        BeginningChargeEndDate = row["StartDate"]
        continue

    else:
        ChargeEndDate.append(BeginningChargeEndDate)

df['ChargeEndDate'] = ChargeEndDate

# Create ChargeLength Column
df["ChargeEndDate"] = pd.to_datetime(df["ChargeEndDate"])
df["ChargeLength"] = ((df["ChargeEndDate"] - df["StartDate"]).astype('timedelta64[M]')) + 1

# Create MrrTotal Column
df = df.sort_index()
MrrTotal = []

BeginningMrrValue = 0

for index, row in df.iterrows():
    if row['ChargeOrder'] == 1:
        MrrTotal.append(row["OrderMrrDelta"])
        BeginningMrrValue = row["OrderMrrDelta"]
        continue
    elif row['ChargeOrder'] != 1:
        MrrTotal.append(BeginningMrrValue + row["OrderMrrDelta"])
        BeginningMrrValue = BeginningMrrValue + row["OrderMrrDelta"]
        continue

    else:
        MrrTotal.append(row["OrderMrrDelta"])

df['MrrTotal'] = MrrTotal

# Load and Clean Up Elp Dataframe
Elp_df = pd.read_csv('/Users/bcraft/Documents/subscription_overview/app/OrderElp.csv')
Elp_df = Elp_df.drop(columns=["OrderElp.Type", "OrderElp.GeneratedReason", "Subscription.Name",
                              "SubscriptionVersionAmendment.Code", "RatePlanCharge.Name", "OrderAction.Type",
                              "Order.OrderNumber", "OrderElp.CreatedDate"])
Elp_df = Elp_df.rename(index=str, columns={"OrderElp.StartDate": "StartDate", "OrderElp.EndDate": "EndDate",
                                           "OrderElp.Value": "ELP", "RatePlanCharge.ChargeNumber": "ChargeNumber"})

# Enforce datetime
Elp_df["StartDate"] = pd.to_datetime(Elp_df["StartDate"])
Elp_df["EndDate"] = pd.to_datetime(Elp_df["EndDate"])

# # Add the Elp_df data frame to og df

df = pd.merge(df, Elp_df, how='left', on=['ChargeNumber', 'StartDate', 'EndDate'])
df = df.fillna(0)

# #Load and Clean Up TCB Dataframe
TCB_df = pd.read_csv('/Users/bcraft/Documents/subscription_overview/app/OrderTcb.csv')
TCB_df = TCB_df.drop(columns=["OrderTcb.Type", "OrderTcb.GeneratedReason", "Subscription.Name",
                              "SubscriptionVersionAmendment.Code", "RatePlanCharge.Name", "Order.OrderNumber"])
TCB_df = TCB_df.rename(index=str, columns={"OrderTcb.StartDate": "StartDate", "OrderTcb.EndDate": "EndDate",
                                           "OrderTcb.Value": "TCBDelta", "OrderAction.Type": "Type",
                                           "RatePlanCharge.ChargeNumber": "ChargeNumber"})
# Enforce DateTime
TCB_df["StartDate"] = pd.to_datetime(TCB_df["StartDate"])
TCB_df["EndDate"] = pd.to_datetime(TCB_df["EndDate"])

# Remove Duplicates in TCB
# Create a df of duplicate rows
Duplicate_rows = TCB_df
Duplicate_rows = Duplicate_rows.groupby(["ChargeNumber", "StartDate", "EndDate"]).agg({'Type': 'count',
                                                                                       'TCBDelta': 'sum'})
Duplicate_rows = Duplicate_rows.rename(index=str, columns={"TCBDelta": "DuplicateTCBDeltaTotal", "Type": "Duplicate"})
Duplicate_rows = Duplicate_rows[Duplicate_rows.Duplicate != 1].reset_index()
Duplicate_rows["Duplicate"] = 1

# Enforce DateTime
Duplicate_rows["StartDate"] = pd.to_datetime(Duplicate_rows["StartDate"])
Duplicate_rows["EndDate"] = pd.to_datetime(Duplicate_rows["EndDate"])

# Join Duplicate rows to TCB df
TCB_df = pd.merge(TCB_df, Duplicate_rows, how='left', on=['ChargeNumber', 'StartDate', 'EndDate'])

# Determine which Duplicate row to keep
BeginningChargeNumber = 0

delete = []

for index, row in TCB_df.iterrows():
    if row['Duplicate'] == 1.0 and row['ChargeNumber'] != BeginningChargeNumber:
        delete.append(0)

        BeginningChargeNumber = row['ChargeNumber']
        continue
    elif row['Duplicate'] == 1.0 and row['ChargeNumber'] == BeginningChargeNumber:
        delete.append(1)
        continue
    else:
        delete.append(0)

TCB_df['delete'] = delete

# Replaced Group by Total and Delta and Remove Duplicates Identified
TCB_df.loc[TCB_df.Duplicate == 1.0, ['TCBDelta']] = TCB_df["DuplicateTCBDeltaTotal"]
TCB_df = TCB_df[TCB_df.delete != 1].reset_index().drop(
    columns=["Duplicate", "delete", "DuplicateTCBDeltaTotal", "index", "Type"])
TCB_df = TCB_df.drop(columns=[])

# Join TCB onto main df
df = pd.merge(df, TCB_df, how='left', on=['ChargeNumber', 'StartDate', 'EndDate'])

# Create TCBTotal
df["TCBTotal"] = df["MrrTotal"] * df["ChargeLength"]

# #Load and Clean Up Quantity Dataframe
Quantity_df = pd.read_csv('/Users/bcraft/Documents/subscription_overview/app/OrderQuantity.csv')

Quantity_df = Quantity_df.drop(columns=["RatePlanCharge.Name", "Account.Name", "Account.Currency",
                                        "SubscriptionVersionAmendment.Code", "Subscription.Name",
                                        "OrderQuantity.CreatedDate", "OrderQuantity.GeneratedReason",
                                        "Order.OrderNumber"])
Quantity_df = Quantity_df.rename(index=str,
                                 columns={"OrderQuantity.StartDate": "StartDate", "OrderQuantity.EndDate": "EndDate",
                                          "OrderQuantity.Value": "QuantityDelta", "OrderAction.Type": "Type",
                                          "RatePlanCharge.ChargeNumber": "ChargeNumber"})

# Enforce DateTime
Quantity_df["StartDate"] = pd.to_datetime(Quantity_df["StartDate"])
Quantity_df["EndDate"] = pd.to_datetime(Quantity_df["EndDate"])

# Remove Duplicates in TCB
# Create a df of duplicate rows
Duplicate_rows = Quantity_df
Duplicate_rows = Duplicate_rows.groupby(["ChargeNumber", "StartDate", "EndDate"]).agg({'Type': 'count',
                                                                                       'QuantityDelta': 'sum'})
Duplicate_rows = Duplicate_rows.rename(index=str, columns={"QuantityDelta": "DuplicateQuantityDeltaTotal",
                                                           "Type": "Duplicate"})

Duplicate_rows = Duplicate_rows[Duplicate_rows.Duplicate != 1].reset_index()
Duplicate_rows["Duplicate"] = 1
# print(Duplicate_rows.to_string())

# Enforce DateTime
Duplicate_rows["StartDate"] = pd.to_datetime(Duplicate_rows["StartDate"])
Duplicate_rows["EndDate"] = pd.to_datetime(Duplicate_rows["EndDate"])

# Join Duplicate rows to TCB df
Quantity_df = pd.merge(Quantity_df, Duplicate_rows, how='left', on=['ChargeNumber', 'StartDate', 'EndDate'])

# Determine which Duplicate row to keep
BeginningChargeNumber = 0

delete = []

for index, row in Quantity_df.iterrows():
    if row['Duplicate'] == 1.0 and row['ChargeNumber'] != BeginningChargeNumber:
        delete.append(0)

        BeginningChargeNumber = row['ChargeNumber']
        continue
    elif row['Duplicate'] == 1.0 and row['ChargeNumber'] == BeginningChargeNumber:
        delete.append(1)
        continue
    else:
        delete.append(0)

Quantity_df['delete'] = delete

# Replaced Group by Total and Delta and Remove Duplicates Identified
Quantity_df.loc[Quantity_df.Duplicate == 1.0, ['QuantityDelta']] = Quantity_df["DuplicateQuantityDeltaTotal"]
Quantity_df = Quantity_df[Quantity_df.delete != 1].reset_index().drop(
    columns=["Duplicate", "delete", "DuplicateQuantityDeltaTotal", "index", "Type"])
Quantity_df = Quantity_df.drop(columns=[])

# Join Quantity_df onto main df
df = pd.merge(df, Quantity_df, how='left', on=['ChargeNumber', 'StartDate', 'EndDate'])
df = df.fillna(0)

# Create QuantityTotal Column
df = df.sort_index()

QuantityTotal = []

BeginningQuantityValue = 0

for index, row in df.iterrows():
    if row['ChargeOrder'] == 1:
        QuantityTotal.append(row["QuantityDelta"])
        BeginningQuantityValue = row["QuantityDelta"]
        continue
    elif row['ChargeOrder'] != 1:
        QuantityTotal.append(BeginningQuantityValue + row["QuantityDelta"])
        BeginningQuantityValue = BeginningQuantityValue + row["QuantityDelta"]
        continue

    else:
        QuantityTotal.append(row["QuantityDelta"])

df['QuantityTotal'] = QuantityTotal

# Resort DF
df = df[['ChargeOrder', 'StartDate', 'ChargeEndDate', 'EndDate', 'ChargeLength', 'Type', 'GeneratedReason',
         'SubscriptionName', 'AmendmentCode', 'ChargeNumber', 'RatePlanChargeName', 'OrderActionType', 'OrderNumber',
         'AccountName', 'AccountCurrency', 'OrderMrrDelta', 'MrrTotal', 'ELP', 'TCBDelta', 'TCBTotal', 'QuantityDelta',
         'QuantityTotal']]


@app.route('/')
@app.route('/index')

def index():
  return df.to_csv(header = 1, index = False, sep=',', line_terminator='\n' )