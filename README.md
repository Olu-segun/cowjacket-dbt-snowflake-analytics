🐄 CowJacket dbt Cloud Analytics Engineering
This repository contains the dbt (data build tool) project for CowJacket, designed to standardize data transformations, automate testing, and provide full lineage visibility using a three-layer modeling approach.

🏗 Project Architecture & Modeling
We follow the industry-standard three-layer modeling architecture to ensure modularity and data quality:

1. Staging (models/staging/): Clean, 1-to-1 mirroring of source data (SQL module data) with basic renaming and type casting.

2. Intermediate (models/intermediate/): Complex joins and business logic that serve as building blocks for the final marts.

3. Marts (models/marts/): Final, analytics-ready models organized by business entity (e.g., customers, orders).
