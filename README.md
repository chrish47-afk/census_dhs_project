# Objective
To analyze trends in immigration and their impact on U.S. demographics, economy, and societal dynamics using US census and DHS data.

---

## Datasets

- **1. The American Community Survey (ACS5)**  
- **2. Homeland Security Statistics (HSS), Yearbook 2023**

## Languages

- **Python**: Census Data Pull and Data Prepping  
- **Scala (Azure Databricks)**: Data Processing and Analysis  
- **Excel**: Vetting and Visualizations  

## Message

I’m a dreamer, an immigrant, and someone who loves this country deeply. My intention with this project is not to spark political debate or discomfort but to fulfill the goals of this Big Data project: to extract, process, and present meaningful insights from publicly available data.

## Why Did I Use These Datasets?

I chose these datasets because they provide comprehensive, reliable, and publicly available data on various demographic, economic, and immigration-related metrics. They align with my project goal to analyze and present findings on the dynamics of immigration in the US using trusted sources. Both datasets are accessible, easy to retrieve, and well-suited for replication during peer review.

## What Are These Datasets?

**The American Community Survey (ACS5):**
- Conducted annually by the U.S. Census Bureau.
- Provides detailed demographic, social, economic, and housing data.
- Covers geographic levels such as national, state, county, and subdivisions.
- Surveys 3.5 million addresses annually, informing federal fund distribution.
- Includes estimates of counts, averages, medians, proportions, and rates with margins of error (MOE).  
*(More details: [Census.gov](https://www.census.gov))*

**Homeland Security Statistics (HSS), Yearbook 2023:**
- Compiles immigration statistics, including lawful permanent residents, naturalizations, and enforcement actions.
- Serves as an authoritative source on U.S. immigration trends.  
*(More details: [OHSS](https://ohss.dhs.gov/topics/immigration/yearbook-immigration-statistics/yearbook-2023))*

---

## Data Pull and Prepping

- **Initial Exploration**: Familiarized myself with the Census library in Python, reviewing documentation, exploring variables, and running initial data pulls for one state and one variable to understand the dataset's structure.  
- **Scaling Up**: Advanced to pulling data for multiple variables, counties, states, and years, requiring complex Python code for effective data retrieval.  
- **Challenges**:
  - Inconsistent variable availability across years.  
  - Unreliable API for large pulls (150+ variables).  
  - Data retrieval took 5–6 hours, highlighting the need for optimization.  

---

## Data Processing and Analysis

### Population and Demographics
- **How does the foreign-born population vary across U.S. counties?**  
  - Variables: `B05001_001E` (Total population), `B05001_006E` (Not a U.S. citizen)  
- **What percentage of the foreign-born population has naturalized citizenship?**  
  - Variables: `B05001_002E` (Naturalized citizens), `B05001_003E` (Non-citizens)  
- **What is the racial and ethnic breakdown of the foreign-born population?**  
  - Variables: `B03002_003E`, `B03002_004E`, `B02001_002E`, `B02001_003E`  
- **How has the foreign-born population changed over the past decade?**  
  - Variables: `B05001_001E`, `B05002_001E`  

---

## Looking Ahead

- **Validation**: Vetting selected Census variables for alignment with research objectives.  
- **Data Integration**: Incorporating all data for targeted locations, years, and variables.  
- **Expanded Analysis**: Exploring Economic Impact, Education, Policy Trends, and Housing.  
- **Visualization**: Translating data insights into engaging visuals.  

---

## Challenges and Roadblocks

- **Complexity**: Navigating the Census API and datasets required significant effort.  
- **Variables**: Reviewing 20,000+ Census variables was time-intensive.  
- **Automation**: Optimizing processes for efficient, large-scale data pulls remains a challenge.  

---

## References

1. [Census ACS Microdata Documentation (2022)](https://www.census.gov/programs-surveys/acs/microdata/documentation/2022.html)  
2. [Census Developer Resources](https://www.census.gov/data/developers.html)  
3. [Intro to Census Data API](https://www.census.gov/data/academy/courses/intro-to-the-census-bureau-data-api.html)  
4. [ACS5 Variables List](https://api.census.gov/data/2022/acs/acs5/variables.html)  
5. [Census Python Library](https://pypi.org/project/census/)  
6. [HSS Yearbook of Immigration Statistics (2023)](https://ohss.dhs.gov/topics/immigration/yearbook-immigration-statistics/yearbook-2023)  
7. [ACS Data Guidance](https://www.census.gov/programs-surveys/acs/guidance/estimates.html)  
8. [API Gateway for ACS5](https://proximityone.com/apigateway_acs5year.htm)  
