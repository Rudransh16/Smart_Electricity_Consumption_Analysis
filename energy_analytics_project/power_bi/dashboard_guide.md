# Power BI dashboard guide

## Import
Import the four Gold exports from Databricks:
- `gold_state_consumption`
- `gold_monthly_trends`
- `gold_peak_demand`
- `gold_power_shortage`

Each Spark CSV export is a folder. Download the `part-*.csv` file inside each folder.

## Data model
Use `gold_state_consumption` as the state-level summary. For cross-table filtering, create a State dimension containing unique state names and relate it one-to-many to:
- `gold_state_consumption[state]`
- `gold_peak_demand[state]`
- `gold_power_shortage[state]`

Create a Date dimension if you want consistent date filtering across monthly and daily tables. Relate it to the relevant date/date key after adding a proper first-of-month date to monthly data.

## Page 1 — Executive Overview
- KPI cards: Total Energy Supplied (MU), Maximum Peak Demand (MW), Total Peak Unmet (MW), States Covered.
- Clustered bar: total energy supplied by state.
- Line chart: monthly energy supplied.
- Slicers: State, Year, Month.

## Page 2 — State-wise Analysis
- Bar chart: top states by total energy supplied.
- Table: state, total energy, average daily energy, maximum peak demand, total peak unmet.
- Map only if reliable state geographic fields are available.

## Page 3 — Monthly Trends
- Line chart: monthly total energy.
- Column chart: monthly maximum peak demand.
- Slicers: Year and Month.

## Page 4 — Peak Demand & Shortage
- Line chart: daily peak met by date.
- Bar chart: total peak unmet by state.
- Table: state, year, month, shortage days, maximum shortage.

## Design tips
- Keep units in every visual title.
- Sort months by month number, not alphabetically by month name.
- Show the dataset date range and source on the report.
- Distinguish missing values from zero values.
- Do not label energy supplied as total electricity consumption unless the dataset definition supports that interpretation.
