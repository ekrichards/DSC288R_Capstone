# Model Cards

## Model Card: Linear Regression

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Regression (Predicting Delay Duration in Minutes)
- **Training Algorithm:** Linear Regression (Scikit-learn)
- **License:** Open-source (for academic use)

### Intended Use:
- **Primary Use:** Predicting delay duration (in minutes) for scheduled flights.
- **Primary Users:** Airlines, airport authorities, and flight scheduling services.
- **Out-of-Scope Use Cases:** Not optimized for extreme weather conditions without further tuning.

### Factors:
- **Relevant Features:** Flight distance, departure time, weather conditions, carrier history.
- **Evaluation Factors:** Predictive accuracy in delay estimation.

### Metrics:
- **Performance Metrics:**
  - Mean Absolute Error (MAE): 36.65 minutes.
  - Root Mean Squared Error (RMSE): 72.99 minutes.
  - R2 Score: 0.06, suggesting poor fit due to non-linearity.
- **Variation Approaches:** Performance varies depending on seasonal trends.

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data.
- **Motivation:** Improve delay prediction accuracy to assist scheduling.
- **Preprocessing:** Missing value imputation, categorical encoding, feature scaling.

### Training Data:
- **Details:**
  - Train-test split: 80%-20%.
  - Feature selection applied, but non-linearity remains a challenge.
  - Alternative models (XGBoost, LightGBM) showed improved performance; good baseline.

### Quantitative Analyses:
- **Unitary Results:** Linear regression struggled with capturing non-linear trends.
- **Intersectional Results:** Regression errors were higher in flights with extreme delays.

### Ethical Considerations:
- **Bias Risks:** Underprediction of severe delays due to high skew.
- **Mitigation:** Consider integrating non-linear models for improved accuracy.

---

## Model Card: Stochastic Gradient Descent Regression

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Regression
- **Training Algorithm:** Stochastic Gradient Descent (scikit-learn)
- **License:** Open Source

### Intended Use:
- **Primary Use:** Predicting the delay (in minutes) of a flight
- **Primary Users:** Airlines, Airports, Passengers, Pilots
- **Out-of-Scope Use Cases:** Not to be used for all predictions of delays

### Factors:
- **Relevant Features:** Distance, Airtime, and airline
- **Evaluation Factors:** Predictive accuracy

### Metrics:
- **Performance Metrics:**
  - Mean Absolute Error (MAE): 36.8
  - Root Mean Squared Error (RMSE): 73.11
  - R2 Score: 0.06
- **Variation Approaches:** Evaluated across different years, airlines, and locations

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data
- **Motivation:** Improve delay prediction accuracy to assist scheduling
- **Preprocessing:** Missing value imputation, categorical encoding, feature scaling

### Training Data:
- **Details:**
  - Train-test split: 80%-20%

### Quantitative Analyses:
- **Unitary Results:** The regression model struggled with capturing nonlinear trends in flight delays, heavily weighting past delay averages (past_50_avg_delay) while underutilizing other flight characteristics such as distance and airtime.
- **Intersectional Results:** Performance varied significantly depending on flight route and seasonality. Longer flights exhibited higher error margins, and flights scheduled during peak travel periods had less reliable predictions.

### Ethical Considerations:
- **Bias Risks:** Underprediction of severe delays due to high skew
- **Mitigation:** Applied undersampling techniques to balance delay instances

---

## Model Card: Histogram-Based Gradient Boosting Regression

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Regression (Predicting Delay Duration in Minutes)
- **Training Algorithm:** Histogram-Based Gradient Boosting Regression (Scikit-learn)
- **License:** Open-source (for academic use)

### Intended Use:
- **Primary Use:** Predicting delay duration (in minutes) for scheduled flights.
- **Primary Users:** Airlines, airport authorities, and flight scheduling services.
- **Out-of-Scope Use Cases:** Not optimized for extreme weather conditions without further tuning.

### Factors:
- **Relevant Features:** Flight distance, departure time, weather conditions, carrier history.
- **Evaluation Factors:** Predictive accuracy in delay estimation.

### Metrics:
- **Performance Metrics:**
  - Mean Absolute Error (MAE): 36.23 minutes.
  - Root Mean Squared Error (RMSE): 72.32 minutes.
  - R2 Score: 0.08, suggesting poor fit.
- **Variation Approaches:** Performance varies depending on seasonal trends.

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data.
- **Motivation:** Improve delay prediction accuracy to assist scheduling.
- **Preprocessing:** Missing value imputation, categorical encoding, feature scaling.

### Training Data:
- **Details:**
  - Train-test split: 80%-20%.

### Quantitative Analyses:
- **Disparities:** Model accuracy varies based on time-of-day, with early morning flights exhibiting better predictions than late-night departures.
- **Feature Importance Analysis:** Weather conditions and historical carrier delay rates are key contributors.

### Ethical Considerations:
- **Bias Risks:** Underprediction of severe delays due to high skew.
- **Mitigation:** Applied undersampling techniques to balance extreme delay instances.

---

## Model Card: Multi-Layer Perceptron Regressor

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Regression
- **Training Algorithm:** Multi-Layer Perceptron (scikit-learn)
- **License:** Open Source

### Intended Use:
- **Primary Use:** Predicting the delay (in minutes) of a flight
- **Primary Users:** Airlines, Airports, Passengers, Pilots
- **Out-of-Scope Use Cases:** Not to be used for all predictions of delays

### Factors:
- **Relevant Features:** Distance, Airtime, past flight delay trends, airline
- **Evaluation Factors:** Predictive accuracy

### Metrics:
- **Performance Metrics:**
  - Mean Absolute Error (MAE): 35.2
  - Root Mean Squared Error (RMSE): 70.85
  - R2 Score: 0.10
- **Variation Approaches:** Evaluated across different years, airlines, and locations

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data
- **Motivation:** Improve delay prediction accuracy to assist scheduling
- **Preprocessing:** Missing value imputation, categorical encoding, feature scaling

### Training Data:
- **Details:**
  - Train-test split: 80%-20%

### Quantitative Analyses:
- **Unitary Results:** The MLP regressor provided a marginal improvement in capturing delay variability over linear models, but still struggled with extreme delay cases.
- **Intersectional Results:** Performance was weaker for longer-haul flights, and delays during weather-affected periods were underpredicted compared to normal conditions.

### Ethical Considerations:
- **Bias Risks:** Model may be less reliable in predicting extreme outliers, potentially disadvantaging flights affected by systemic disruptions.
- **Mitigation:** Consider incorporating real-time weather and air traffic control data to improve extreme event predictions.

---

## Model Card: Logistic Regression

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Classification
- **Training Algorithm:** Logistic Regression (Scikit-learn)
- **License:** Open Source

### Intended Use:
- **Primary Use:** Predicting the delay of a flight.
- **Primary Users:** Airlines, Airports, Passengers, Pilots.
- **Out-of-Scope Use Cases:** Not to be used for all predictions of delays.

### Factors:
- **Relevant Features:** Distance, Airtime, and airline.
- **Evaluation Factors:** Confusion Matrix, delay prediction accuracy.

### Metrics:
- **Performance Metrics:**
  - Accuracy: 0.64
  - Precision: 0.65
  - Recall: 0.61
  - F1 Score: 0.65
- **Decision Thresholds:** Standard binary threshold (0.5 probability split).
- **Variation Approaches:** Evaluated across different years, airlines, and locations.

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data.
- **Motivation:** Improve delay prediction accuracy to assist scheduling.
- **Preprocessing:** Missing value imputation, categorical encoding, feature scaling.

### Training Data:
- **Details:**
  - Train-test split: 80%-20%.

### Quantitative Analyses:
- **Unitary Results:** The model relied heavily on departure time and distance for predictions, potentially overlooking real-time operational factors.
- **Intersectional Results:** Prediction accuracy varied by airline and time of day, with some airlines experiencing higher misclassification rates.

### Ethical Considerations:
- **Bias Risks:** Underprediction of severe delays due to high skew.
- **Mitigation:** Applied undersampling techniques to balance delay instances.

---

## Model Card: Stochastic Gradient Descent Classifier

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Classification
- **Training Algorithm:** Stochastic Gradient Descent (scikit-learn)
- **License:** Open Source

### Intended Use:
- **Primary Use:** Predicting the delay of a flight
- **Primary Users:** Airlines, Airports, Passengers, Pilots
- **Out-of-Scope Use Cases:** Not to be used for all predictions of delays

### Factors:
- **Relevant Features:** Distance, Airtime, and airline
- **Evaluation Factors:** Predictive accuracy

### Metrics:
- **Performance Metrics:**
  - Accuracy: 0.64
  - Precision: 0.64
  - Recall: 0.65
  - F1 Score: 0.65
- **Variation Approaches:** Evaluated across different years, airlines, and locations

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data
- **Motivation:** Improve delay prediction accuracy to assist scheduling
- **Preprocessing:** Missing value imputation, categorical encoding, feature scaling

### Training Data:
- **Details:**
  - Train-test split: 80%-20%

### Quantitative Analyses:
- **Unitary Results:** The model relied heavily on past delay trends (past_50_avg_delay) for predictions, indicating that historical delays play a stronger role than distance or airtime in forecasting delays.
- **Intersectional Results:** The model performed inconsistently across airlines, with some carriers experiencing higher misclassification rates. Time of day also impacted performance, as delays occurring later in the day were often underpredicted.

### Ethical Considerations:
- **Bias Risks:** Underprediction of severe delays due to high skew
- **Mitigation:** Applied undersampling techniques to balance delay instances

---

## Model Card: Histogram-Based Gradient Boosting Classifier

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Classification (Binary - Delayed vs. On-time)
- **Training Algorithm:** Histogram-Based Gradient Boosting Classifier (Scikit-learn)
- **License:** Open-source (for academic use)

### Intended Use:
- **Primary Use:** Predicting whether a flight will be delayed or not.
- **Primary Users:** Airlines, airport management, and passengers.
- **Out-of-Scope Use Cases:** Not designed for real-time, operational scheduling without further optimization.

### Factors:
- **Relevant Features:** Weather conditions, departure time, carrier, origin/destination airports, holiday indicators, historical delay trends.
- **Evaluation Factors:** Delay prediction accuracy, false positive/negative rates.

### Metrics:
- **Performance Metrics:**
  - Accuracy: 0.66
  - Precision: 0.68
  - Recall: 0.64
  - F1 Score: 0.66
- **Decision Thresholds:** Standard binary threshold (0.5 probability split).
- **Variation Approaches:** Evaluated across different time periods and airport locations.

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data.
- **Motivation:** Real-world flight delay prediction, improving scheduling and planning.
- **Preprocessing:** Missing value imputation, target encoding, and categorical transformations.

### Training Data:
- **Details:**
  - Train-test split: 80%-20%.
  - Undersampling performed to address class imbalance.
  - Feature importance analyzed but could be further refined.

### Quantitative Analyses:
- **Unitary Results:** High classification accuracy but potential data leakage concerns.
- **Intersectional Results:** Performance varies across airlines and flight routes.

### Ethical Considerations:
- **Bias Risks:** Model performance may vary based on carrier and airport locations.
- **Mitigation:** Ensuring fairness by evaluating error rates across different airlines.

---

## Model Card: Multi-Layer Perceptron Classifier

### Model Details:
- **Model Date:** 2025
- **Model Version:** 1.0
- **Model Type:** Classification
- **Training Algorithm:** Multi-Layer Perceptron (scikit-learn)
- **License:** Open Source

### Intended Use:
- **Primary Use:** Predicting the delay of a flight
- **Primary Users:** Airlines, Airports, Passengers, Pilots
- **Out-of-Scope Use Cases:** Not to be used for all predictions of delays

### Factors:
- **Relevant Features:** Distance, Airtime, past flight delay trends, airline
- **Evaluation Factors:** Predictive accuracy

### Metrics:
- **Performance Metrics:**
  - Accuracy: 0.67
  - Precision: 0.68
  - Recall: 0.66
  - F1 Score: 0.67
- **Variation Approaches:** Evaluated across different years, airlines, and locations

### Evaluation Data:
- **Datasets:** Kaggle Flight Delay Dataset (2018-2022), NOAA Weather Data
- **Motivation:** Improve delay prediction accuracy to assist scheduling
- **Preprocessing:** Missing value imputation, categorical encoding, feature scaling

### Training Data:
- **Details:**
  - Train-test split: 80%-20%

### Quantitative Analyses:
- **Unitary Results:** The model captured non-linear relationships in delay prediction better than simpler classifiers, leveraging flight history and timing patterns effectively.
- **Intersectional Results:** Accuracy varied based on the airline and flight distance, with short-haul flights being more prone to misclassification. Flights departing in peak congestion periods had higher false positive rates.

### Ethical Considerations:
- **Bias Risks:** Over-reliance on past delays could reinforce systemic scheduling inefficiencies.
- **Mitigation:** Regular model retraining to adapt to changing flight patterns and congestion.
