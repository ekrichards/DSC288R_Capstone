# Data Card: Flight Delay, Airport, and Weather Datasets

## Dataset Details:

- **Name:** Flight Delay, OpenFlights, and NOAA Weather Combined Dataset  
- **Version:** 1.0  
- **Coverage:** 2018-2022 (Global, U.S. Focus)  
- **License:** Refer to individual dataset terms  

## Intended Use:

- **Primary Use:** Flight delay analysis, predictive modeling, operational insights.  
- **Users:** Airlines, airport authorities, transportation analysts, researchers.  
- **Out-of-Scope:** Real-time scheduling, aviation safety risk analysis.  

## Factors:

### Features:
- **Flight Data:** Airline, flight number, departure/arrival times, delay, cancellation.  
- **Airport Data:** ICAO/IATA codes, name, location.  
- **Weather Data:** Temperature, precipitation, wind speed, visibility.  

### Evaluation:
- Data completeness, timestamp alignment, consistency checks.  

## Metrics:

- **Data Quality:** Missing values, data balance, consistency.  

## Evaluation Data:

- **Datasets:** Kaggle Flight Delay, OpenFlights, NOAA GHCN  
- **Motivation:** Analyzing flight delays impacted by operational and weather factors.  
- **Preprocessing:** Handling missing data, merging sources by timestamps and location codes.  

## Training Data (For Predictive Models):

- **Techniques:** Data balancing, feature selection, correlation analysis.  

## Quantitative Analyses:

- **Unitary Results:** Delay trends, airline & seasonal impact.  
- **Intersectional Results:** Variation by airport, route, weather conditions.  

## Ethical Considerations:

- **Bias Risks:** Data skewed toward major airports and airlines.  
- **Mitigation:** Analyzing fairness across airlines/regions, handling missing data properly.  
