# Objective
To analyze trends in immigration and their impact on U.S. demographics, economy, and societal dynamics using US census and DHS data.

---
# Datasets

- **The American Community Survey (ACS5)**
- **Homeland Security Statistics (HSS), Yearbook 2023**

# Languages

- **Python**: Census Data Pull and Data Prepping  
- **Scala (Azure Databricks)**: Data Processing and Analysis  
- **Excel**: Vetting and Visualizations

# Message
I’m a dreamer, an immigrant, and someone who loves this country deeply. My intention with this project is not to spark political debate or discomfort, but to fulfill the goals of this Big Data project: to extract, process, and present meaningful insights from publicly available data.

# Why Did I Use These Datasets?
I chose these datasets because they provide comprehensive, reliable, and publicly available data on a variety of demographic, economic, and immigration-related metrics. They align with my project goal to analyze and present findings on the dynamics of immigration in the U.S. using trusted sources. Both data sources are readily accessible, straightforward to retrieve, and therefore well-suited for replication for peer review if necessary.

# What Are These Datasets?

### The American Community Survey (ACS5)
- The ACS is an annual survey conducted by the U.S. Census Bureau. It provides detailed demographic, social, economic, and housing data at various geographic levels, such as national, state, county, and smaller subdivisions.
- The nation’s most current, reliable, and accessible data source for local statistics on critical planning topics such as age, children, veterans, commuting, education, income, and employment.
- Surveys 3.5 million addresses and informs how trillions of federal funds are distributed each year.
- Covers 40+ topics and supports over 300 evidence-based Federal government uses.
- ACS5 is the most reliable, precise, and representative. It may be the least current, but when it comes to precision and reliability, ACS5 is the best dataset to use.
- The ACS5 dataset presents estimates of counts, averages, medians, proportions, and rates, derived from survey sampling and accompanied by margins of error (MOE) to quantify uncertainty.
- For more details please see [Census.gov | U.S. Census Bureau Homepage](https://www.census.gov/).
- **Note: ACS1 vs ACS5 vs Decennial Datasets:** ACS1 provides annual, timely estimates for larger populations (65,000+). ACS5 offers more reliable estimates with greater geographic granularity by combining five years of data, while the Decennial Census is a comprehensive population count every 10 years, serving as the baseline for apportionment and funding allocation.

### Homeland Security Statistics (HSS), Yearbook 2023
- The HSS Yearbook compiles immigration-related statistics, including lawful permanent residents, naturalizations, non-immigrant admissions, and enforcement actions. It is published by the Department of Homeland Security (DHS).
- The 2023 Yearbook of Immigration Statistics is a collection of tables about immigration for the fiscal year.
- For more details please see [About Our Data | OHSS - Office of Homeland Security Statistics](https://ohss.dhs.gov/).

---
# Data Pull and Prepping

### Initial Exploration
- Began by familiarizing myself with the census library in Python, including reviewing documentation, exploring the variable glossary, and running basic data pulls for one state and one variable at a time to understand the structure and capabilities of the dataset.

### Scaling Up
- Progressed to pulling data for multiple variables, multiple counties, and multiple states across all available ACS5 years. This required writing more advanced and complex Python code to handle larger-scale data retrieval effectively.

**Insights from Expanded Data Pull:**
- Utilized the Census API and the `us` library to retrieve ACS5 data for all counties, states, and years.
- A wide range of variables was included, selected based on personal interest and some chosen randomly.
- Inconsistencies emerged where certain variables were missing for specific years, resulting in potential gaps in the final dataset.
- The API proved unreliable at times, occasionally failing when too many variables were requested in a single pull.
- The data retrieval process was time-intensive, taking approximately 5-6 hours to complete, highlighting a need for script optimization to improve efficiency or scaling down the variable list.

# Data Processing and Analysis

### Population and Demographics

- **How does the foreign-born population vary across different counties in the U.S.?**  
  Variables: `B05001_001E` (Total population in the United States), `B05001_006E` (Total population in the United States, Not a U.S. citizen)

- **What percentage of the foreign-born population has naturalized citizenship?**  
  Variables: `B05001_002E` (Naturalized Citizens), `B05001_003E` (Non-Citizens)

- **What is the racial and ethnic breakdown of the foreign-born population in the U.S.?**  
  Variables: `B03002_003E` (Hispanic or Latino), `B03002_004E` (White alone, not Hispanic), `B02001_002E` (White), `B02001_003E` (Black or African American)

- **How has the foreign-born population changed over the past decade in various counties?**  
  Variables: `B05001_001E`, `B05002_001E`

---
# Looking Ahead

- **Validating Variables:** A deeper vetting process is required to ensure the selected Census variables align with the intended research questions and objectives.
- **Comprehensive Data Integration:** Incorporating all available data for the targeted locations, years, and variables to create a robust and inclusive dataset.
- **Expanding Data Analysis:** Developing and addressing additional questions focusing on areas such as Economic Impact, Education, Policy and Migration Trends, and Housing.
- **Enhanced Visualizations:** Translating data findings into clear, engaging visual narratives to better communicate insights and trends.

# Challenges and Roadblocks

- **Complexity of Census Data:** The Census API and datasets are more intricate than initially anticipated, requiring significant time to understand their structure and limitations.
- **Navigating Vast Variables:** With over 20,000 variables to explore, identifying and selecting the most relevant ones proved labor-intensive despite the availability of glossaries and documentation.
- **Coding and Optimization:** Automating processes and optimizing the code to handle large-scale data pulls posed ongoing challenges, especially in ensuring efficiency and reliability during execution.

---
# References
1. [Census Documentation 2022](https://www.census.gov/programs-surveys/acs/microdata/documentation/2022.html)  
2. [Census Data API Developer Guide](https://www.census.gov/data/developers.html)  
3. [Intro to Census Bureau Data API](https://www.census.gov/data/academy/courses/intro-to-the-census-bureau-data-api.html)  
4. [ACS5 Variables](https://api.census.gov/data/2022/acs/acs5/variables.html)  
5. [Python Census Library](https://pypi.org/project/census/)  
6. [DHS Yearbook of Immigration Statistics 2023](https://ohss.dhs.gov/topics/immigration/yearbook-immigration-statistics/yearbook-2023)  
7. [ACS Guidance on Estimates](https://www.census.gov/programs-surveys/acs/guidance/estimates.html)  
8. [ACS5 API Gateway](https://proximityone.com/apigateway_acs5year.htm)
